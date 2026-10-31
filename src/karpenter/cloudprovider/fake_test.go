// In-memory fake of the plugin NodeClaim API and fixtures shared by the provider unit tests.

package cloudprovider

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// fakeAPI records calls and serves a fixed catalog; capacityErrors lists "type/zone/capacity" triples that fail
// with InsufficientCapacity, and quotaErrors those that fail with it for a quota.
type fakeAPI struct {
	mu             sync.Mutex
	catalog        nodeclaim.Catalog
	catalogErr     error
	catalogCalls   int
	claims         map[string]*nodeclaim.NodeClaim
	creates        []nodeclaim.CreateRequest
	deletes        []string
	capacityErrors map[string]bool
	quotaErrors    map[string]bool
	listErr        error
	createErr      error
	launchZone     string        // zone a claim without one lands in, as the cloud picks it
	omitStorage    bool          // launched claims lack ephemeral-storage, as when the plugin has not read the VM's disk yet
	refusalDelay   time.Duration // how long a capacity refusal takes, as EC2 answers a refusal slower than a launch
	onRefusal      func()        // runs before each capacity refusal returns
	nextID         int
}

func newFakeAPI(catalog nodeclaim.Catalog) *fakeAPI {
	return &fakeAPI{catalog: catalog, claims: map[string]*nodeclaim.NodeClaim{}, capacityErrors: map[string]bool{}, quotaErrors: map[string]bool{}}
}

func (f *fakeAPI) Catalog(_ context.Context, regions []string) (*nodeclaim.Catalog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.catalogCalls++
	if f.catalogErr != nil {
		return nil, f.catalogErr
	}
	want := map[string]bool{}
	for _, r := range regions {
		want[r] = true
	}
	out := &nodeclaim.Catalog{}
	for _, it := range f.catalog.InstanceTypes {
		cp := it
		cp.Offerings = nil
		for _, o := range it.Offerings {
			if len(want) == 0 || want[o.Region] {
				cp.Offerings = append(cp.Offerings, o)
			}
		}
		out.InstanceTypes = append(out.InstanceTypes, cp)
	}
	return out, nil
}

func (f *fakeAPI) Create(_ context.Context, req *nodeclaim.CreateRequest) (*nodeclaim.NodeClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, *req)
	if existing, ok := f.claims[req.Name]; ok {
		return existing, nil
	}
	if f.createErr != nil {
		return nil, f.createErr
	}
	if f.quotaErrors[req.InstanceType+"/"+cmpZone(req)+"/"+req.CapacityType] {
		return nil, &nodeclaim.APIError{Status: http.StatusServiceUnavailable, Reason: nodeclaim.ReasonInsufficientCapacity, Cause: nodeclaim.CauseQuota, Message: "VcpuLimitExceeded"}
	}
	if f.capacityErrors[req.InstanceType+"/"+cmpZone(req)+"/"+req.CapacityType] {
		f.mu.Unlock()
		time.Sleep(f.refusalDelay)
		if f.onRefusal != nil {
			f.onRefusal()
		}
		f.mu.Lock()
		return nil, &nodeclaim.APIError{Status: http.StatusServiceUnavailable, Reason: nodeclaim.ReasonInsufficientCapacity, Message: "no capacity"}
	}
	f.nextID++
	// Like the plugin, the fake reports the launched type's hardware and the VM's root volume.
	hw := nodeclaim.Hardware{EphemeralStorage: fakeRootVolume, Pods: 110}
	for _, it := range f.catalog.InstanceTypes {
		if it.Name == req.InstanceType {
			hw.CPU, hw.Memory, hw.GPUs = it.CPU, it.Memory, it.GPUCount
		}
	}
	if f.omitStorage {
		hw.EphemeralStorage = ""
	}
	nc := &nodeclaim.NodeClaim{
		Name:         req.Name,
		ProviderID:   nodeclaim.FormatProviderID(req.Annotations["cloudvm.vk.io/provider"], req.Region, fmt.Sprintf("i-%04d", f.nextID)),
		InstanceType: req.InstanceType,
		Region:       req.Region,
		Zone:         cmp.Or(req.Zone, f.launchZone),
		CapacityType: req.CapacityType,
		State:        nodeclaim.StateLaunching,
		CreatedAt:    time.Unix(1_700_000_000, 0).UTC(),
		Labels:       req.Labels,
		Taints:       req.Taints,
		Capacity:     hw.Capacity(),
	}
	f.claims[req.Name] = nc
	return nc, nil
}

// fakeRootVolume is the root volume size of every VM the fake launches.
const fakeRootVolume = "200Gi"

func cmpZone(req *nodeclaim.CreateRequest) string {
	if req.Zone == "" {
		return req.Region
	}
	return req.Zone
}

func (f *fakeAPI) Get(_ context.Context, name string) (*nodeclaim.NodeClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if nc, ok := f.claims[name]; ok {
		return nc, nil
	}
	return nil, notFound()
}

func (f *fakeAPI) GetByProviderID(_ context.Context, providerID string) (*nodeclaim.NodeClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, nc := range f.claims {
		if nc.ProviderID == providerID {
			return nc, nil
		}
	}
	return nil, notFound()
}

func (f *fakeAPI) List(context.Context) ([]nodeclaim.NodeClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]nodeclaim.NodeClaim, 0, len(f.claims))
	for _, nc := range f.claims {
		out = append(out, *nc)
	}
	return out, nil
}

func (f *fakeAPI) Delete(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, name)
	if _, ok := f.claims[name]; !ok {
		return notFound()
	}
	delete(f.claims, name)
	return nil
}

func notFound() error {
	return &nodeclaim.APIError{Status: http.StatusNotFound, Reason: nodeclaim.ReasonNotFound, Message: "not found"}
}

// testCatalog has a GPU type offered in two AWS regions, a CPU type, and a GCP GPU type.
func testCatalog() nodeclaim.Catalog {
	return nodeclaim.Catalog{InstanceTypes: []nodeclaim.InstanceType{
		{
			Name: "g6.2xlarge", Provider: "aws", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4",
			GPUMemoryGB: 24, GPUManufacturer: "NVIDIA", Architecture: "x86_64", Family: "g6", Generation: "6", Size: "2xlarge",
			Offerings: []nodeclaim.Offering{
				{Region: "us-east-1", Zone: "us-east-1a", CapacityType: "spot", PricePerHour: 0.40, Available: true},
				{Region: "us-east-1", Zone: "us-east-1b", CapacityType: "on-demand", PricePerHour: 0.98, Available: true},
				{Region: "eu-west-1", Zone: "eu-west-1a", CapacityType: "spot", PricePerHour: 0.30, Available: true},
				{Region: "ap-south-1", Zone: "ap-south-1a", CapacityType: "spot", PricePerHour: 0.10, Available: true},
			},
		},
		{
			Name: "m7i.xlarge", Provider: "aws", CPU: "4", Memory: "16Gi", Architecture: "amd64",
			Offerings: []nodeclaim.Offering{
				{Region: "us-east-1", Zone: "us-east-1a", CapacityType: "on-demand", PricePerHour: 0.20, Available: true},
			},
		},
		{
			Name: "g2-standard-8", Provider: "gcp", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4",
			GPUMemoryGB: 24, GPUManufacturer: "nvidia", Architecture: "amd64", Family: "g2",
			Offerings: []nodeclaim.Offering{
				{Region: "us-central1", Zone: "us-central1-a", CapacityType: "spot", PricePerHour: 0.25, Available: true},
			},
		},
	}}
}
