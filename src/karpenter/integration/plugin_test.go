// HTTP stand-in for the plugin's NodeClaim API, served to the real nodeclaim client in the integration test.

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// pluginServer records every launch and termination it receives.
type pluginServer struct {
	*httptest.Server

	mu      sync.Mutex
	catalog nodeclaim.Catalog
	claims  map[string]*nodeclaim.NodeClaim
	creates []nodeclaim.CreateRequest
	deletes []string
}

// pluginToken is the only bearer token the stand-in plugin accepts.
const pluginToken = "provider-service-account-token"

func newPluginServer(catalog nodeclaim.Catalog) *pluginServer {
	p := &pluginServer{catalog: catalog, claims: map[string]*nodeclaim.NodeClaim{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+nodeclaim.PathCatalog, func(w http.ResponseWriter, _ *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		writeJSON(w, http.StatusOK, p.catalog)
	})
	mux.HandleFunc("POST "+nodeclaim.PathNodeClaims, p.create)
	mux.HandleFunc("GET "+nodeclaim.PathNodeClaims, p.list)
	mux.HandleFunc("GET "+nodeclaim.PathNodeClaims+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		nc, ok := p.claims[r.PathValue("name")]
		if !ok {
			writeJSON(w, http.StatusNotFound, nodeclaim.ErrorResponse{Reason: nodeclaim.ReasonNotFound})
			return
		}
		writeJSON(w, http.StatusOK, nc)
	})
	mux.HandleFunc("DELETE "+nodeclaim.PathNodeClaims+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		name := r.PathValue("name")
		p.deletes = append(p.deletes, name)
		if _, ok := p.claims[name]; !ok {
			writeJSON(w, http.StatusNotFound, nodeclaim.ErrorResponse{Reason: nodeclaim.ReasonNotFound})
			return
		}
		delete(p.claims, name)
		w.WriteHeader(http.StatusAccepted)
	})
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+pluginToken {
			writeJSON(w, http.StatusUnauthorized, nodeclaim.ErrorResponse{Reason: "Unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	}))
	return p
}

func (p *pluginServer) create(w http.ResponseWriter, r *http.Request) {
	var req nodeclaim.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Validate() != nil {
		writeJSON(w, http.StatusBadRequest, nodeclaim.ErrorResponse{Reason: nodeclaim.ReasonInvalidRequest})
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.creates = append(p.creates, req)
	if existing, ok := p.claims[req.Name]; ok {
		writeJSON(w, http.StatusOK, existing)
		return
	}
	nc := &nodeclaim.NodeClaim{
		Name:         req.Name,
		ProviderID:   nodeclaim.FormatProviderID(req.Annotations["cloudvm.vk.io/provider"], req.Region, fmt.Sprintf("i-%04d", len(p.creates))),
		InstanceType: req.InstanceType,
		Region:       req.Region,
		Zone:         req.Zone,
		CapacityType: req.CapacityType,
		State:        nodeclaim.StateLaunching,
		CreatedAt:    time.Now().UTC(),
		Labels:       req.Labels,
		Taints:       req.Taints,
		Capacity:     map[string]string{"cpu": "8", "memory": "32Gi", "pods": "110", nodeclaim.ResourceGPU: "1"},
	}
	p.claims[req.Name] = nc
	writeJSON(w, http.StatusCreated, nc)
}

func (p *pluginServer) list(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := nodeclaim.List{Items: []nodeclaim.NodeClaim{}}
	providerID := r.URL.Query().Get("providerID")
	for _, nc := range p.claims {
		if providerID == "" || nc.ProviderID == providerID {
			out.Items = append(out.Items, *nc)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// launches returns the distinct NodeClaim names the plugin launched VMs for.
func (p *pluginServer) launches() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, c := range p.creates {
		if !seen[c.Name] {
			seen[c.Name] = true
			out = append(out, c.Name)
		}
	}
	return out
}

func (p *pluginServer) deleted() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.deletes...)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func gpuCatalog() nodeclaim.Catalog {
	offering := func(region, zone, capacityType string, price float64) nodeclaim.Offering {
		return nodeclaim.Offering{Region: region, Zone: zone, CapacityType: capacityType, PricePerHour: price, Available: true}
	}
	return nodeclaim.Catalog{InstanceTypes: []nodeclaim.InstanceType{{
		Name: "g6.2xlarge", Provider: "aws", CPU: "8", Memory: "32Gi", GPUCount: 1, GPUModel: "l4", GPUMemoryGB: 24,
		GPUManufacturer: "nvidia", Architecture: "amd64", Family: "g6", Generation: "6", Size: "2xlarge",
		Offerings: []nodeclaim.Offering{
			offering("us-east-1", "us-east-1a", "spot", 0.40),
			offering("eu-west-1", "eu-west-1a", "spot", 0.30),
			offering("eu-west-1", "eu-west-1b", "on-demand", 0.90),
		},
	}}}
}
