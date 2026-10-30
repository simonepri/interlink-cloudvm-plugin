// The serving certificate approver: approves the CertificateSigningRequests node agent pods make for their kubelet
// API, and only those whose subject, usages, lifetime and IP addresses match the requesting pod; it denies the rest.

package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"slices"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Keys of the user extra the API server records for a pod-bound service account token.
const (
	podNameExtra = "authentication.kubernetes.io/pod-name"
	podUIDExtra  = "authentication.kubernetes.io/pod-uid"
)

// allowedServingUsages are the key usages a kubelet serving certificate may carry.
var allowedServingUsages = []certificatesv1.KeyUsage{
	certificatesv1.UsageDigitalSignature, certificatesv1.UsageKeyEncipherment, certificatesv1.UsageServerAuth,
}

// ServingCertApproverConfig configures the approver.
type ServingCertApproverConfig struct {
	Kube kubernetes.Interface
	// SignerName is the only signer whose requests the approver reviews.
	SignerName string
	// AgentNamespace and AgentServiceAccount identify the node agent pods whose requests it approves.
	AgentNamespace, AgentServiceAccount string
	// MaxDuration bounds the requested certificate lifetime.
	MaxDuration time.Duration
}

// ServingCertApprover reviews CertificateSigningRequests for one signer.
type ServingCertApprover struct {
	cfg      ServingCertApproverConfig
	username string
}

// NewServingCertApprover returns an approver for the configured node agent service account.
func NewServingCertApprover(cfg ServingCertApproverConfig) *ServingCertApprover {
	return &ServingCertApprover{
		cfg:      cfg,
		username: fmt.Sprintf("system:serviceaccount:%s:%s", cfg.AgentNamespace, cfg.AgentServiceAccount),
	}
}

// Run approves or denies the signer's pending requests as they appear until ctx ends.
func (a *ServingCertApprover) Run(ctx context.Context) error {
	factory := informers.NewSharedInformerFactoryWithOptions(a.cfg.Kube, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) {
			o.FieldSelector = fields.OneTermEqualSelector("spec.signerName", a.cfg.SignerName).String()
		}))
	csrs := factory.Certificates().V1().CertificateSigningRequests()
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())
	enqueue := func(obj any) {
		if csr, ok := obj.(*certificatesv1.CertificateSigningRequest); ok && !decided(csr) {
			queue.Add(csr.Name)
		}
	}
	if _, err := csrs.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj any) { enqueue(obj) },
	}); err != nil {
		return err
	}
	factory.Start(ctx.Done())
	defer factory.Shutdown()
	if !cache.WaitForCacheSync(ctx.Done(), csrs.Informer().HasSynced) {
		return ctx.Err()
	}
	go func() {
		<-ctx.Done()
		queue.ShutDown()
	}()
	logf("approving %s requests of %s", a.cfg.SignerName, a.username)
	for {
		name, shutdown := queue.Get()
		if shutdown {
			return nil
		}
		csr, err := csrs.Lister().Get(name)
		if err == nil {
			err = a.sync(ctx, csr)
		}
		switch {
		case err == nil || apierrors.IsNotFound(err):
			queue.Forget(name)
		default:
			logf("reviewing certificate request %s: %v", name, err)
			queue.AddRateLimited(name)
		}
		queue.Done(name)
	}
}

// sync approves or denies one request; it leaves requests of other signers and users untouched.
func (a *ServingCertApprover) sync(ctx context.Context, csr *certificatesv1.CertificateSigningRequest) error {
	if decided(csr) || csr.Spec.SignerName != a.cfg.SignerName || csr.Spec.Username != a.username {
		return nil
	}
	problem, err := a.review(ctx, csr)
	if err != nil {
		return err
	}
	csr = csr.DeepCopy()
	condition := certificatesv1.CertificateSigningRequestCondition{
		Type: certificatesv1.CertificateApproved, Status: corev1.ConditionTrue, Reason: "CloudVMNodeAgentServingCert",
		Message: "The node agent pod requested a serving certificate for its own IP addresses.",
	}
	if problem != "" {
		condition.Type, condition.Reason, condition.Message = certificatesv1.CertificateDenied, "CloudVMNodeAgentMismatch", problem
	}
	csr.Status.Conditions = append(csr.Status.Conditions, condition)
	if _, err := a.cfg.Kube.CertificatesV1().CertificateSigningRequests().UpdateApproval(ctx, csr.Name, csr,
		metav1.UpdateOptions{}); err != nil {
		return err
	}
	logf("%s certificate request %s of pod %s: %s", condition.Type, csr.Name, single(csr.Spec.Extra[podNameExtra]),
		condition.Message)
	return nil
}

// review returns why the request must be denied, or "" to approve it; an error means it could not decide yet.
func (a *ServingCertApprover) review(ctx context.Context, csr *certificatesv1.CertificateSigningRequest) (string, error) {
	podName, podUID := single(csr.Spec.Extra[podNameExtra]), single(csr.Spec.Extra[podUIDExtra])
	if podName == "" || podUID == "" {
		return "The request was not made with a pod-bound service account token.", nil
	}
	pod, err := a.cfg.Kube.CoreV1().Pods(a.cfg.AgentNamespace).Get(ctx, podName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Sprintf("Pod %s does not exist.", podName), nil
	case err != nil:
		return "", err
	case string(pod.UID) != podUID:
		return fmt.Sprintf("Pod %s is not the pod that made the request.", podName), nil
	}
	return checkServingRequest(csr, pod, a.cfg.SignerName, a.cfg.MaxDuration), nil
}

// checkServingRequest returns why the request does not match a serving certificate for the pod, or "" if it does.
func checkServingRequest(csr *certificatesv1.CertificateSigningRequest, pod *corev1.Pod, signer string,
	maxDuration time.Duration) string {
	if !slices.Contains(csr.Spec.Usages, certificatesv1.UsageServerAuth) {
		return "The request lacks the server auth usage."
	}
	for _, usage := range csr.Spec.Usages {
		if !slices.Contains(allowedServingUsages, usage) {
			return fmt.Sprintf("The request asks for the %s usage.", usage)
		}
	}
	if csr.Spec.ExpirationSeconds == nil || time.Duration(*csr.Spec.ExpirationSeconds)*time.Second > maxDuration {
		return fmt.Sprintf("The request must set expirationSeconds to at most %d.", int64(maxDuration.Seconds()))
	}
	block, _ := pem.Decode(csr.Spec.Request)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return "The request holds no PEM certificate request."
	}
	req, err := x509.ParseCertificateRequest(block.Bytes)
	if err == nil {
		err = req.CheckSignature()
	}
	if err != nil {
		return fmt.Sprintf("The certificate request is invalid: %v.", err)
	}
	if want := servingCertSubject(signer, pod.Name); req.Subject.String() != want.String() {
		return fmt.Sprintf("The subject must be %q.", want.String())
	}
	if len(req.DNSNames) > 0 || len(req.EmailAddresses) > 0 || len(req.URIs) > 0 {
		return "The request may name only IP addresses."
	}
	if len(req.IPAddresses) == 0 {
		return "The request names no IP address."
	}
	for _, ip := range req.IPAddresses {
		if !slices.ContainsFunc(pod.Status.PodIPs, func(p corev1.PodIP) bool { return ip.Equal(net.ParseIP(p.IP)) }) {
			return fmt.Sprintf("IP address %s is not an address of pod %s.", ip, pod.Name)
		}
	}
	return ""
}

// decided reports whether a request is already approved, denied or failed.
func decided(csr *certificatesv1.CertificateSigningRequest) bool {
	return slices.ContainsFunc(csr.Status.Conditions, func(c certificatesv1.CertificateSigningRequestCondition) bool {
		return c.Type == certificatesv1.CertificateApproved || c.Type == certificatesv1.CertificateDenied ||
			c.Type == certificatesv1.CertificateFailed
	})
}

// single returns the only value of a user extra, or "" when it has none or several.
func single(values certificatesv1.ExtraValue) string {
	if len(values) != 1 {
		return ""
	}
	return values[0]
}
