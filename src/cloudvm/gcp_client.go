// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	compute "google.golang.org/api/compute/v1"
)

// GCPComputeService defines an interface wrapping the GCP Compute Engine instances API
// to enable unit testing and mocking.
type GCPComputeService interface {
	Insert(ctx context.Context, project string, zone string, instance *compute.Instance) (*compute.Operation, error)
	Delete(ctx context.Context, project string, zone string, instance string) (*compute.Operation, error)
	Get(ctx context.Context, project string, zone string, instance string) (*compute.Instance, error)
	List(ctx context.Context, project string, zone string, filter string) (*compute.InstanceList, error)
	WaitZoneOperation(ctx context.Context, project string, zone string, operation string) error
}

// gcpComputeServiceImpl wraps google.golang.org/api/compute/v1 InstancesService and ZoneOperationsService.
type gcpComputeServiceImpl struct {
	instances      *compute.InstancesService
	zoneOperations *compute.ZoneOperationsService
}

// NewGCPComputeService wraps a compute.InstancesService and compute.ZoneOperationsService.
func NewGCPComputeService(instances *compute.InstancesService, ops *compute.ZoneOperationsService) GCPComputeService {
	return &gcpComputeServiceImpl{
		instances:      instances,
		zoneOperations: ops,
	}
}

// NewGCPComputeServiceFromService wraps a compute.Service.
func NewGCPComputeServiceFromService(service *compute.Service) GCPComputeService {
	if service == nil {
		return nil
	}
	return &gcpComputeServiceImpl{
		instances:      service.Instances,
		zoneOperations: service.ZoneOperations,
	}
}

// NewDefaultGCPComputeService creates a GCPComputeService using Application Default Credentials.
func NewDefaultGCPComputeService(ctx context.Context) (GCPComputeService, error) {
	svc, err := compute.NewService(ctx)
	if err != nil {
		return nil, err
	}
	return NewGCPComputeServiceFromService(svc), nil
}

func (s *gcpComputeServiceImpl) Insert(ctx context.Context, project string, zone string, instance *compute.Instance) (*compute.Operation, error) {
	return s.instances.Insert(project, zone, instance).Context(ctx).Do()
}

func (s *gcpComputeServiceImpl) Delete(ctx context.Context, project string, zone string, instance string) (*compute.Operation, error) {
	return s.instances.Delete(project, zone, instance).Context(ctx).Do()
}

func (s *gcpComputeServiceImpl) Get(ctx context.Context, project string, zone string, instance string) (*compute.Instance, error) {
	return s.instances.Get(project, zone, instance).Context(ctx).Do()
}

func (s *gcpComputeServiceImpl) List(ctx context.Context, project string, zone string, filter string) (*compute.InstanceList, error) {
	call := s.instances.List(project, zone)
	if filter != "" {
		call = call.Filter(filter)
	}
	var all []*compute.Instance
	err := call.Pages(ctx, func(page *compute.InstanceList) error {
		if page != nil {
			all = append(all, page.Items...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &compute.InstanceList{Items: all}, nil
}

func (s *gcpComputeServiceImpl) WaitZoneOperation(ctx context.Context, project string, zone string, operation string) error {
	if s.zoneOperations == nil {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		op, err := s.zoneOperations.Get(project, zone, operation).Context(ctx).Do()
		if err != nil {
			return err
		}
		if op.Status == "DONE" {
			if op.Error != nil && len(op.Error.Errors) > 0 {
				var msgs []string
				for _, e := range op.Error.Errors {
					msgs = append(msgs, fmt.Sprintf("%s: %s", e.Code, e.Message))
				}
				return fmt.Errorf("zone operation failed: %s", strings.Join(msgs, "; "))
			}
			return nil
		}
		time.Sleep(1 * time.Second)
	}
}
