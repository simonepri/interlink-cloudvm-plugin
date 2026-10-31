// Package main implements the interlink-cloudvm-plugin daemon.
package main

// This file resolves each region's launch templates, learns the CPU architecture of each template's image, and
// assigns every candidate instance type to the first template whose requirements it satisfies.

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// LaunchTemplateFact is one launch template a region's workers can boot from, with the Karpenter-style instance
// requirements an instance type must satisfy to use it, such as `karpenter.k8s.aws/instance-family NotIn [g6f]`.
type LaunchTemplateFact struct {
	ID           string                           `json:"id,omitempty"`
	Name         string                           `json:"name,omitempty"`
	Requirements []corev1.NodeSelectorRequirement `json:"requirements,omitempty"`
}

// errNoLaunchTemplateFits reports that no launch template of the region accepts any candidate instance type.
var errNoLaunchTemplateFits = errors.New("no launch template of the region accepts any candidate instance type")

// templateArchTTL bounds how long a template's image architecture stays cached; launches use the template's $Latest version.
const templateArchTTL = time.Hour

// templateArchRetry is how soon a failed architecture read is retried, so a throttled call does not pin a guess for an hour.
const templateArchRetry = time.Minute

// noGPUSlices excludes GPU slices, which need NVIDIA's GRID driver that no legacy template is known to ship; only a
// launchTemplates entry boots them.
var noGPUSlices = corev1.NodeSelectorRequirement{Key: labelGPUSlice, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"true"}}

// defaultLaunchTemplate is the template a region whose cluster facts name none boots every type from but GPU slices.
func defaultLaunchTemplate() LaunchTemplateFact {
	return LaunchTemplateFact{Name: defaultLaunchTemplateName, Requirements: []corev1.NodeSelectorRequirement{noGPUSlices}}
}

// regionTemplates returns a region's launch templates in the order launches try them. Without a launchTemplates
// list, the GPU template takes types with whole GPUs and the default template every other type except GPU slices.
func regionTemplates(fact RegionalClusterFact) []LaunchTemplateFact {
	if len(fact.LaunchTemplates) > 0 {
		return fact.LaunchTemplates
	}
	noSlices := noGPUSlices
	var out []LaunchTemplateFact
	if fact.GPULaunchTemplateID != "" {
		out = append(out, LaunchTemplateFact{ID: fact.GPULaunchTemplateID, Requirements: []corev1.NodeSelectorRequirement{
			{Key: labelInstanceGPUCount, Operator: corev1.NodeSelectorOpExists}, noSlices,
		}})
	}
	if fact.LaunchTemplateID != "" || fact.LaunchTemplateName != "" {
		out = append(out, LaunchTemplateFact{ID: fact.LaunchTemplateID, Name: fact.LaunchTemplateName, Requirements: []corev1.NodeSelectorRequirement{noSlices}})
	}
	return out
}

// launchTemplateClient is the EC2 subset that reads a template's image architecture.
type launchTemplateClient interface {
	DescribeLaunchTemplateVersions(ctx context.Context, params *ec2.DescribeLaunchTemplateVersionsInput, optFns ...func(*ec2.Options)) (*ec2.DescribeLaunchTemplateVersionsOutput, error)
	DescribeImages(ctx context.Context, params *ec2.DescribeImagesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
}

type templateArchEntry struct {
	arch    string
	expires time.Time
}

// templateArchCache caches the Kubernetes architecture of each region's template images.
type templateArchCache struct {
	mu      sync.Mutex
	entries map[string]templateArchEntry
}

var globalTemplateArch = &templateArchCache{entries: make(map[string]templateArchEntry)}

// arch returns the Kubernetes architecture of the template's latest image, or "" when EC2 cannot tell, such as for
// an image resolved from an SSM parameter or without permission to read the template.
func (c *templateArchCache) arch(ctx context.Context, client launchTemplateClient, region string, t LaunchTemplateFact) string {
	key := region + "/" + t.ID + "/" + t.Name
	c.mu.Lock()
	if e, ok := c.entries[key]; ok && time.Now().Before(e.expires) {
		c.mu.Unlock()
		return e.arch
	}
	c.mu.Unlock()

	arch, err := readTemplateArch(ctx, client, t)
	ttl := templateArchTTL
	if err != nil {
		ttl = templateArchRetry
		log.Printf("[aws] Reading the image architecture of launch template %s in %s failed: %v", sanitizeLog(t.ID+t.Name), sanitizeLog(region), err) //nolint:gosec // sanitized
	}
	c.mu.Lock()
	c.entries[key] = templateArchEntry{arch: arch, expires: time.Now().Add(ttl)}
	c.mu.Unlock()
	return arch
}

// latestTemplateData returns the data of the template's $Latest version, or nil when EC2 reports none.
func latestTemplateData(ctx context.Context, client launchTemplateClient, t LaunchTemplateFact) (*ec2types.ResponseLaunchTemplateData, error) {
	in := &ec2.DescribeLaunchTemplateVersionsInput{Versions: []string{"$Latest"}}
	if t.ID != "" {
		in.LaunchTemplateId = aws.String(t.ID)
	} else {
		in.LaunchTemplateName = aws.String(t.Name)
	}
	versions, err := client.DescribeLaunchTemplateVersions(ctx, in)
	if err != nil {
		return nil, err
	}
	if len(versions.LaunchTemplateVersions) == 0 {
		return nil, nil
	}
	return versions.LaunchTemplateVersions[0].LaunchTemplateData, nil
}

func readTemplateArch(ctx context.Context, client launchTemplateClient, t LaunchTemplateFact) (string, error) {
	if client == nil {
		return "", nil
	}
	data, err := latestTemplateData(ctx, client, t)
	if err != nil || data == nil {
		return "", err
	}
	image := aws.ToString(data.ImageId)
	if image == "" || strings.HasPrefix(image, "resolve:ssm:") {
		return "", nil
	}
	images, err := client.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{image}})
	if err != nil {
		return "", err
	}
	if len(images.Images) == 0 {
		return "", nil
	}
	switch images.Images[0].Architecture {
	case ec2types.ArchitectureValuesX8664:
		return "amd64", nil
	case ec2types.ArchitectureValuesArm64:
		return "arm64", nil
	}
	return "", nil
}

// resolvedTemplate is a launch template with its full requirements: the declared ones plus its image architecture.
type resolvedTemplate struct {
	LaunchTemplateFact
	reqs scheduling.Requirements
}

// resolveTemplates adds each template's image architecture to its requirements. A template that declares
// kubernetes.io/arch keeps its own; one whose architecture EC2 cannot tell is taken as amd64.
func resolveTemplates(ctx context.Context, client launchTemplateClient, region string, templates []LaunchTemplateFact) []resolvedTemplate {
	out := make([]resolvedTemplate, 0, len(templates))
	for _, t := range templates {
		reqs := scheduling.NewNodeSelectorRequirements(t.Requirements...)
		if !reqs.Has(corev1.LabelArchStable) {
			arch := globalTemplateArch.arch(ctx, client, region, t)
			if arch == "" {
				arch = "amd64"
			}
			reqs.Add(scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, arch))
		}
		out = append(out, resolvedTemplate{LaunchTemplateFact: t, reqs: reqs})
	}
	return out
}

// templateFor returns the first template whose requirements the instance type satisfies, or false.
func templateFor(templates []resolvedTemplate, f InstanceTypeFacts) (resolvedTemplate, bool) {
	typeReqs := f.requirements()
	for _, t := range templates {
		if typeReqs.Compatible(t.reqs, scheduling.AllowUndefinedWellKnownLabels) == nil {
			return t, true
		}
	}
	return resolvedTemplate{}, false
}

// fleetTemplateGroup is one launch template of a fleet request and the instance types it boots.
type fleetTemplateGroup struct {
	ID            string
	Name          string
	InstanceTypes []string
}

// groupByTemplate assigns each instance type to its template, keeping the types' order within each group and the
// templates' order across groups; types no template accepts are left out.
func groupByTemplate(templates []resolvedTemplate, instanceTypes []string, facts map[string]InstanceTypeFacts) []fleetTemplateGroup {
	index := make(map[string]int)
	var groups []fleetTemplateGroup
	for _, name := range instanceTypes {
		f, ok := facts[name]
		if !ok {
			f = staticInstanceFacts(name)
		}
		t, ok := templateFor(templates, f)
		if !ok {
			continue
		}
		key := t.ID + "/" + t.Name
		i, seen := index[key]
		if !seen {
			i = len(groups)
			index[key] = i
			groups = append(groups, fleetTemplateGroup{ID: t.ID, Name: t.Name})
		}
		groups[i].InstanceTypes = append(groups[i].InstanceTypes, name)
	}
	return groups
}

// launchableTypes returns the names of the instance types that some template accepts.
func launchableTypes(templates []resolvedTemplate, facts map[string]InstanceTypeFacts) map[string]bool {
	out := make(map[string]bool, len(facts))
	for name, f := range facts {
		if _, ok := templateFor(templates, f); ok {
			out[name] = true
		}
	}
	return out
}

// LaunchableInstanceTypes returns the names among facts that the region's launch templates boot, the same set a launch
// in the region draws its candidates from.
func (p *AWSProvider) LaunchableInstanceTypes(ctx context.Context, region string, facts map[string]InstanceTypeFacts) map[string]bool {
	var tc launchTemplateClient
	if c, err := p.getClient(ctx, region); err == nil {
		tc, _ = c.(launchTemplateClient)
	}
	return launchableTypes(resolveTemplates(ctx, tc, region, p.regionTemplates(region)), facts)
}
