// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	compute "google.golang.org/api/compute/v1"
)

// GCPProvider implements Provider, CloudInventory, and InstanceStatusChecker for Google Cloud Platform.
type GCPProvider struct {
	Project             string
	Zone                string
	Subnet              string
	ServiceAccountEmail string
	configuredZones     []string
	instanceZones       map[string]string
	client              GCPComputeService
	mu                  sync.RWMutex
}

// NewGCPProvider creates a new GCP Compute Engine provider.
func NewGCPProvider(project, zone string, additionalZones ...string) *GCPProvider {
	if zone == "" {
		zone = "us-central1-a"
	}

	var confZones []string
	seen := make(map[string]bool)
	confZones = append(confZones, zone)
	seen[zone] = true
	for _, z := range additionalZones {
		z = strings.TrimSpace(z)
		if z != "" && !seen[z] {
			confZones = append(confZones, z)
			seen[z] = true
		}
	}

	return &GCPProvider{
		Project:         project,
		Zone:            zone,
		configuredZones: confZones,
		instanceZones:   make(map[string]string),
	}
}

// SetClient overrides the internal GCP compute client (used for testing).
func (p *GCPProvider) SetClient(c GCPComputeService) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.client = c
}

// AddConfiguredZones adds additional zones to be queried during multi-zone inventory scans.
func (p *GCPProvider) AddConfiguredZones(zones ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := make(map[string]bool, len(p.configuredZones))
	for _, z := range p.configuredZones {
		seen[z] = true
	}
	for _, z := range zones {
		z = strings.TrimSpace(z)
		if z != "" && !seen[z] {
			p.configuredZones = append(p.configuredZones, z)
			seen[z] = true
		}
	}
}

func (p *GCPProvider) getClient(ctx context.Context) (GCPComputeService, error) {
	p.mu.RLock()
	if p.client != nil {
		c := p.client
		p.mu.RUnlock()
		return c, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}

	cl, err := NewDefaultGCPComputeService(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP compute client: %w", err)
	}
	p.client = cl
	return cl, nil
}

func (p *GCPProvider) getProject() string {
	if p.Project != "" {
		return p.Project
	}
	if prj := os.Getenv("GCP_PROJECT"); prj != "" {
		return prj
	}
	if prj := os.Getenv("GOOGLE_CLOUD_PROJECT"); prj != "" {
		return prj
	}
	return "default"
}

func (p *GCPProvider) resolveZone(spec *InstanceSpec) string {
	if spec != nil && spec.Region != "" {
		reg := strings.TrimSpace(spec.Region)
		parts := strings.Split(reg, "-")
		if len(parts) >= 3 && len(parts[len(parts)-1]) <= 2 {
			return reg
		}

		p.mu.RLock()
		pZone := p.Zone
		p.mu.RUnlock()
		if strings.HasPrefix(pZone, reg) {
			return pZone
		}
		return reg + "-a"
	}

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.Zone != "" {
		return p.Zone
	}
	return "us-central1-a"
}

func zoneToRegion(zone string) string {
	idx := strings.LastIndex(zone, "-")
	if idx > 0 {
		return zone[:idx]
	}
	return zone
}

func sanitizeGCPName(val string) string {
	val = strings.ToLower(val)
	var sb strings.Builder
	for _, r := range val {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			sb.WriteRune(r)
		} else if r == '-' || r == '_' || r == '.' {
			sb.WriteRune('-')
		}
	}
	return strings.Trim(sb.String(), "-")
}

func sanitizeGCPLabel(val string) string {
	val = strings.ToLower(val)
	var sb strings.Builder
	for _, r := range val {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	res := strings.Trim(sb.String(), "-_")
	if len(res) > 63 {
		res = strings.Trim(res[:63], "-_")
	}
	return res
}

func formatGCPInstanceName(spec *InstanceSpec) string {
	podName := sanitizeGCPName(spec.PodName)
	if podName == "" {
		podName = "worker"
	}
	base := fmt.Sprintf("cvm-%s", podName)
	if spec.PodUID != "" {
		uid := sanitizeGCPName(spec.PodUID)
		if len(uid) > 8 {
			uid = uid[:8]
		}
		base = fmt.Sprintf("cvm-%s-%s", uid, podName)
	}
	if len(base) > 63 {
		base = strings.Trim(base[:63], "-")
	}
	return base
}

func resolveGCPMachineType(spec *InstanceSpec, zone string) string {
	raw := strings.TrimSpace(spec.InstanceType)
	if raw == "" {
		switch {
		case spec.NumGPUs >= 8:
			raw = "a2-highgpu-8g"
		case spec.NumGPUs >= 4:
			raw = "a2-highgpu-4g"
		case spec.NumGPUs >= 1:
			raw = "g2-standard-4"
		default:
			raw = "e2-standard-4"
		}
	}

	for _, suffix := range []string{"-1t4", "-2t4", "-4t4"} {
		if strings.HasSuffix(raw, suffix) {
			raw = strings.TrimSuffix(raw, suffix)
			break
		}
	}

	if !strings.Contains(raw, "/") {
		return fmt.Sprintf("zones/%s/machineTypes/%s", zone, raw)
	}
	return raw
}

func resolveGCPGuestAccelerator(spec *InstanceSpec, zone string) (string, int64) {
	accelType := strings.ToLower(strings.TrimSpace(spec.GPUType))
	count := int64(spec.NumGPUs)

	if accelType == "" {
		itype := strings.ToLower(strings.TrimSpace(spec.InstanceType))
		switch {
		case strings.Contains(itype, "t4"):
			accelType = "nvidia-tesla-t4"
		case strings.HasPrefix(itype, "a2-highgpu") || strings.HasPrefix(itype, "a2-megagpu"):
			accelType = "nvidia-tesla-a100"
		case strings.HasPrefix(itype, "a2-ultragpu"):
			accelType = "nvidia-a100-80gb"
		case strings.HasPrefix(itype, "g2-"):
			accelType = "nvidia-l4"
		case strings.HasPrefix(itype, "v4-"):
			accelType = "tpu-v4-podslice"
		case strings.HasPrefix(itype, "v5e-") || strings.HasPrefix(itype, "v5litepod-"):
			accelType = "tpu-v5-lite-podslice"
		default:
			if spec.NumGPUs > 0 {
				accelType = "nvidia-tesla-t4"
			}
		}

		if count <= 0 {
			if profile, ok := KnownGCPHardwareProfiles[itype]; ok {
				if profile.GPUs > 0 {
					count = int64(profile.GPUs)
				} else if profile.TPUs > 0 {
					count = int64(profile.TPUs)
				}
			}
		}
	} else {
		switch accelType {
		case "t4", "nvidia-t4":
			accelType = "nvidia-tesla-t4"
		case "a100", "nvidia-a100":
			accelType = "nvidia-tesla-a100"
		case "a100-80gb", "nvidia-a100-80gb":
			accelType = "nvidia-a100-80gb"
		case "v100", "nvidia-v100":
			accelType = "nvidia-tesla-v100"
		case "l4", "nvidia-l4":
			accelType = "nvidia-l4"
		case "p100", "nvidia-p100":
			accelType = "nvidia-tesla-p100"
		case "p4", "nvidia-p4":
			accelType = "nvidia-tesla-p4"
		case "tpu-v4", "v4":
			accelType = "tpu-v4-podslice"
		case "tpu-v5e", "v5e", "v5litepod":
			accelType = "tpu-v5-lite-podslice"
		}
	}

	if accelType == "" {
		return "", 0
	}
	if count <= 0 {
		count = 1
	}

	accelURL := accelType
	if !strings.Contains(accelURL, "/") {
		accelURL = fmt.Sprintf("zones/%s/acceleratorTypes/%s", zone, accelType)
	}
	return accelURL, count
}

func mapGCPStatusToState(gcpStatus string) InstanceState {
	switch gcpStatus {
	case "PROVISIONING", "STAGING":
		return StateProvisioning
	case "RUNNING":
		return StateRunning
	case "STOPPING", "DEPROVISIONING":
		return StateTerminating
	case "TERMINATED", "STOPPED", "SUSPENDED":
		return StateTerminated
	default:
		return StateRunning
	}
}

// Launch provisions a Google Compute Engine VM worker and configures its startup script.
func (p *GCPProvider) Launch(ctx context.Context, spec *InstanceSpec) (*Instance, error) {
	if spec != nil {
		itype := strings.ToLower(strings.TrimSpace(spec.InstanceType))
		if spec.NumTPUs > 0 || strings.HasPrefix(itype, "v4-") || strings.HasPrefix(itype, "v5e-") || strings.HasPrefix(itype, "v5litepod-") || strings.HasPrefix(itype, "tpu-") {
			return nil, fmt.Errorf("gcp cloud tpus require the cloud tpu api (tpu.googleapis.com); gce compute instances only support nvidia gpu accelerators")
		}
	}
	zone := p.resolveZone(spec)
	region := zoneToRegion(zone)
	project := p.getProject()
	p.AddConfiguredZones(zone)

	runnerToken := spec.RunnerAuthToken
	if spec.KMSEncryptedToken || spec.KMSCiphertext != "" {
		runnerToken = ""
	}
	cloudInit, err := GenerateCloudInit(&CloudInitConfig{
		PodName:               spec.PodName,
		PodNamespace:          spec.PodNamespace,
		PodUID:                spec.PodUID,
		RayHeadAddress:        spec.RayHeadAddress,
		RayVersion:            spec.RayVersion,
		TargetContainerImage:  spec.TargetContainerImage,
		Command:               spec.Command,
		Args:                  spec.Args,
		Env:                   spec.Env,
		UseNativeDLAMI:        spec.AMIMode == "dlami",
		NumGPUs:               spec.NumGPUs,
		AssignedGPUs:          spec.AssignedGPUs,
		RunnerAuthToken:       runnerToken,
		KMSEncryptedToken:     spec.KMSEncryptedToken,
		KMSKeyID:              spec.KMSKeyID,
		KMSCiphertext:         spec.KMSCiphertext,
		MeshAuthKey:           spec.MeshAuthKey,
		Region:                region,
		InstanceType:          spec.InstanceType,
		EgressBandwidth:       spec.EgressBandwidth,
		EgressBurst:           spec.EgressBurst,
		CPUMillicores:         spec.CPUMillicores,
		MemoryBytes:           spec.MemoryBytes,
		RunAsUser:             spec.RunAsUser,
		RunAsGroup:            spec.RunAsGroup,
		DockerConfigJSON:      spec.DockerConfigJSON,
		RegistryMirrors:       spec.RegistryMirrors,
		OtelEndpoint:          spec.OtelEndpoint,
		NodeProblemDetector:   spec.NodeProblemDetector,
		CustomBootstrapScript: spec.CustomBootstrapScript,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate gcp startup script: %w", err)
	}

	instName := formatGCPInstanceName(spec)
	machineTypeURL := resolveGCPMachineType(spec, zone)

	nic := &compute.NetworkInterface{}
	if spec.EnablePublicIP {
		nic.AccessConfigs = []*compute.AccessConfig{
			{
				Type: "ONE_TO_ONE_NAT",
				Name: "External NAT",
			},
		}
	}
	if len(spec.SubnetIDs) > 0 && spec.SubnetIDs[0] != "" {
		subnet := spec.SubnetIDs[0]
		if !strings.Contains(subnet, "/") {
			subnet = fmt.Sprintf("regions/%s/subnetworks/%s", region, subnet)
		}
		nic.Subnetwork = subnet
	} else {
		nic.Network = "global/networks/default"
	}

	isSpot := spec.MarketType != "on-demand"
	var autoRestart bool
	scheduling := &compute.Scheduling{}
	if isSpot {
		autoRestart = false
		scheduling.Preemptible = true
		scheduling.ProvisioningModel = "SPOT"
		scheduling.OnHostMaintenance = "TERMINATE"
		scheduling.AutomaticRestart = &autoRestart
	} else {
		autoRestart = true
		scheduling.Preemptible = false
		scheduling.ProvisioningModel = "STANDARD"
		scheduling.AutomaticRestart = &autoRestart
		scheduling.OnHostMaintenance = "MIGRATE"
	}

	saEmail := spec.ServiceAccountEmail
	if saEmail == "" {
		saEmail = p.ServiceAccountEmail
	}
	if saEmail == "" {
		saEmail = "default"
	}
	scopes := spec.ServiceAccountScopes
	if len(scopes) == 0 {
		scopes = []string{
			"https://www.googleapis.com/auth/logging.write",
			"https://www.googleapis.com/auth/monitoring.write",
		}
	}
	serviceAccounts := []*compute.ServiceAccount{
		{
			Email:  saEmail,
			Scopes: scopes,
		},
	}

	clusterTag := spec.ClusterTag
	if clusterTag == "" {
		clusterTag = "interlink"
	}
	labels := map[string]string{
		"cluster_tag": sanitizeGCPLabel(clusterTag),
		"pod_name":    sanitizeGCPLabel(spec.PodName),
		"namespace":   sanitizeGCPLabel(spec.PodNamespace),
		"pod_uid":     sanitizeGCPLabel(spec.PodUID),
		"managed_by":  "interlink-cloudvm-plugin",
	}
	if spec.CostCenter != "" {
		labels["cost_center"] = sanitizeGCPLabel(spec.CostCenter)
	}
	if spec.JobID != "" {
		labels["job_id"] = sanitizeGCPLabel(spec.JobID)
	}

	var guestAccelerators []*compute.AcceleratorConfig
	itype := strings.ToLower(strings.TrimSpace(spec.InstanceType))
	isIntegratedGPU := strings.HasPrefix(itype, "a2-") || strings.HasPrefix(itype, "g2-") || strings.HasPrefix(itype, "a3-")
	if !isIntegratedGPU {
		accelURL, accelCount := resolveGCPGuestAccelerator(spec, zone)
		if accelURL != "" && accelCount > 0 {
			guestAccelerators = append(guestAccelerators, &compute.AcceleratorConfig{
				AcceleratorType:  accelURL,
				AcceleratorCount: accelCount,
			})
			scheduling.OnHostMaintenance = "TERMINATE"
		}
	} else {
		scheduling.OnHostMaintenance = "TERMINATE"
	}

	sourceImage := ""
	if spec.RegionalAMI != nil {
		sourceImage = spec.RegionalAMI[region]
		if sourceImage == "" {
			sourceImage = spec.RegionalAMI[zone]
		}
	}
	if sourceImage == "" && spec.LaunchTemplateName != "" && (strings.HasPrefix(spec.LaunchTemplateName, "projects/") || strings.HasPrefix(spec.LaunchTemplateName, "global/images/")) {
		sourceImage = spec.LaunchTemplateName
	}
	if sourceImage == "" {
		sourceImage = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
	}

	disks := []*compute.AttachedDisk{
		{
			Boot:       true,
			AutoDelete: true,
			Type:       "PERSISTENT",
			InitializeParams: &compute.AttachedDiskInitializeParams{
				SourceImage: sourceImage,
				DiskSizeGb:  50,
				DiskType:    fmt.Sprintf("zones/%s/diskTypes/pd-balanced", zone),
			},
		},
	}

	metadata := &compute.Metadata{
		Items: []*compute.MetadataItems{
			{
				Key:   "startup-script",
				Value: &cloudInit,
			},
		},
	}

	gcpInstance := &compute.Instance{
		Name:              instName,
		MachineType:       machineTypeURL,
		NetworkInterfaces: []*compute.NetworkInterface{nic},
		Scheduling:        scheduling,
		ServiceAccounts:   serviceAccounts,
		Labels:            labels,
		GuestAccelerators: guestAccelerators,
		Disks:             disks,
		Metadata:          metadata,
	}

	client, err := p.getClient(ctx)
	if err != nil {
		return nil, err
	}

	op, err := client.Insert(ctx, project, zone, gcpInstance)
	if err != nil {
		return nil, fmt.Errorf("failed to insert gcp instance %s: %w", instName, err)
	}
	if op != nil && op.Name != "" {
		if err := client.WaitZoneOperation(ctx, project, zone, op.Name); err != nil {
			return nil, fmt.Errorf("gcp instance creation failed for %s: %w", instName, err)
		}
	}

	createdInst, err := client.Get(ctx, project, zone, instName)
	if err != nil {
		return nil, fmt.Errorf("failed to get created gcp instance %s: %w", instName, err)
	}

	var internalIP string
	if createdInst != nil {
		for _, iface := range createdInst.NetworkInterfaces {
			if iface != nil && iface.NetworkIP != "" {
				internalIP = iface.NetworkIP
				break
			}
		}
	}

	p.mu.Lock()
	if p.instanceZones == nil {
		p.instanceZones = make(map[string]string)
	}
	p.instanceZones[instName] = zone
	p.mu.Unlock()

	marketType := "spot"
	if !isSpot {
		marketType = "on-demand"
	}

	return &Instance{
		ID:           instName,
		Provider:     ProviderGCP,
		Region:       region,
		InstanceType: spec.InstanceType,
		IP:           internalIP,
		State:        StateRunning,
		CreatedAt:    time.Now(),
		Metadata: map[string]string{
			"marketType":  marketType,
			"cloud":       "gcp",
			"podName":     spec.PodName,
			"namespace":   spec.PodNamespace,
			"podUID":      spec.PodUID,
			"runnerToken": spec.RunnerAuthToken,
			"clusterTag":  spec.ClusterTag,
			"zone":        zone,
		},
	}, nil
}

// Terminate deletes a Google Compute Engine VM worker.
func (p *GCPProvider) Terminate(ctx context.Context, instanceID string) error {
	p.mu.RLock()
	zone, ok := p.instanceZones[instanceID]
	p.mu.RUnlock()
	if !ok || zone == "" {
		zone = p.Zone
	}

	client, err := p.getClient(ctx)
	if err != nil {
		return err
	}

	project := p.getProject()
	_, err = client.Delete(ctx, project, zone, instanceID)
	if err == nil {
		p.mu.Lock()
		delete(p.instanceZones, instanceID)
		p.mu.Unlock()
	}
	return err
}

// Status queries the live status of a Google Compute Engine instance.
func (p *GCPProvider) Status(ctx context.Context, instanceID string) (*Instance, error) {
	p.mu.RLock()
	zone, ok := p.instanceZones[instanceID]
	p.mu.RUnlock()
	if !ok || zone == "" {
		zone = p.Zone
	}

	client, err := p.getClient(ctx)
	if err != nil {
		return nil, err
	}

	project := p.getProject()
	gcpInst, err := client.Get(ctx, project, zone, instanceID)
	if err != nil {
		return nil, err
	}

	return p.toCloudVMInstance(gcpInst, zone), nil
}

// ListActiveInstancesByRegion queries GCP instances across all configured zones grouped by region.
func (p *GCPProvider) ListActiveInstancesByRegion(ctx context.Context, clusterTag string) (map[string][]*Instance, map[string]error) {
	client, err := p.getClient(ctx)
	if err != nil {
		return nil, map[string]error{"client": err}
	}

	project := p.getProject()
	p.mu.RLock()
	zonesMap := make(map[string]bool)
	if p.Zone != "" {
		zonesMap[p.Zone] = true
	}
	for _, z := range p.configuredZones {
		if z != "" {
			zonesMap[z] = true
		}
	}
	for _, z := range p.instanceZones {
		if z != "" {
			zonesMap[z] = true
		}
	}
	p.mu.RUnlock()

	var filter string
	if clusterTag != "" {
		filter = fmt.Sprintf("labels.cluster_tag = \"%s\"", sanitizeGCPLabel(clusterTag))
	}

	instancesByRegion := make(map[string][]*Instance)
	errorsByRegion := make(map[string]error)

	for zone := range zonesMap {
		reg := zoneToRegion(zone)
		list, err := client.List(ctx, project, zone, filter)
		if err != nil {
			errorsByRegion[zone] = fmt.Errorf("failed to list instances in zone %s: %w", zone, err)
			continue
		}
		if list == nil {
			continue
		}

		for _, item := range list.Items {
			if item.Status == "TERMINATED" || item.Status == "STOPPED" {
				continue
			}
			inst := p.toCloudVMInstance(item, zone)
			instancesByRegion[reg] = append(instancesByRegion[reg], inst)

			p.mu.Lock()
			if p.instanceZones == nil {
				p.instanceZones = make(map[string]string)
			}
			p.instanceZones[item.Name] = zone
			p.mu.Unlock()
		}
	}

	return instancesByRegion, errorsByRegion
}

// ListActiveInstances returns all active cloud instances tagged with the cluster identifier.
func (p *GCPProvider) ListActiveInstances(ctx context.Context, clusterTag string) ([]*Instance, error) {
	instancesByRegion, errorsByRegion := p.ListActiveInstancesByRegion(ctx, clusterTag)
	var allInstances []*Instance
	for _, insts := range instancesByRegion {
		allInstances = append(allInstances, insts...)
	}

	if len(instancesByRegion) == 0 && len(errorsByRegion) > 0 {
		var errMsgs []string
		for zone, err := range errorsByRegion {
			errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", zone, err))
		}
		return nil, fmt.Errorf("all zones failed active instance query: %s", strings.Join(errMsgs, "; "))
	}

	for zone, err := range errorsByRegion {
		cleanZone := sanitizeGCPName(zone)
		log.Printf("[gcp] Warning: zone %s failed inventory query: %v", cleanZone, err) //nolint:gosec // zone sanitized
	}

	return allInstances, nil
}

func (p *GCPProvider) toCloudVMInstance(gcpInst *compute.Instance, zone string) *Instance {
	if gcpInst == nil {
		return nil
	}

	region := zoneToRegion(zone)
	if gcpInst.Zone != "" {
		parts := strings.Split(gcpInst.Zone, "/")
		lastPart := parts[len(parts)-1]
		if lastPart != "" {
			zone = lastPart
			region = zoneToRegion(zone)
		}
	}

	itype := gcpInst.MachineType
	if strings.Contains(itype, "/") {
		parts := strings.Split(itype, "/")
		itype = parts[len(parts)-1]
	}

	var ip string
	if len(gcpInst.NetworkInterfaces) > 0 {
		nic := gcpInst.NetworkInterfaces[0]
		ip = nic.NetworkIP
		if ip == "" && len(nic.AccessConfigs) > 0 {
			ip = nic.AccessConfigs[0].NatIP
		}
	}

	state := mapGCPStatusToState(gcpInst.Status)

	var createdAt time.Time
	if gcpInst.CreationTimestamp != "" {
		if t, err := time.Parse(time.RFC3339, gcpInst.CreationTimestamp); err == nil {
			createdAt = t
		}
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	marketType := "on-demand"
	if gcpInst.Scheduling != nil && (gcpInst.Scheduling.Preemptible || gcpInst.Scheduling.ProvisioningModel == "SPOT") {
		marketType = "spot"
	}

	meta := make(map[string]string)
	meta["cloud"] = "gcp"
	meta["zone"] = zone
	meta["marketType"] = marketType
	if gcpInst.Labels != nil {
		if v, ok := gcpInst.Labels["cluster_tag"]; ok {
			meta["clusterTag"] = v
		}
		if v, ok := gcpInst.Labels["pod_name"]; ok {
			meta["podName"] = v
		}
		if v, ok := gcpInst.Labels["namespace"]; ok {
			meta["namespace"] = v
		}
		if v, ok := gcpInst.Labels["pod_uid"]; ok {
			meta["podUID"] = v
		}
	}

	return &Instance{
		ID:           gcpInst.Name,
		Provider:     ProviderGCP,
		Region:       region,
		InstanceType: itype,
		IP:           ip,
		State:        state,
		CreatedAt:    createdAt,
		Metadata:     meta,
	}
}
