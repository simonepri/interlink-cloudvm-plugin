// Client implements API over the plugin's HTTP endpoints, and APIError carries the reason of a non-2xx response so
// callers can tell missing claims and capacity shortages from other failures.

package nodeclaim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// maxErrorBody bounds how much of an error response the client reads.
const maxErrorBody = 64 << 10

// APIError is a non-2xx response of the plugin.
type APIError struct {
	Status  int
	Reason  string
	Cause   string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("nodeclaim API: HTTP %d %s: %s", e.Status, e.Reason, e.Message)
}

func hasReason(err error, reason string) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Reason == reason
}

// IsNotFound reports whether err says the claim does not exist.
func IsNotFound(err error) bool { return hasReason(err, ReasonNotFound) }

// IsUserDataTooLarge reports whether err says the VM's boot script is larger than the cloud accepts, which no other
// offering changes.
func IsUserDataTooLarge(err error) bool { return hasReason(err, ReasonUserDataTooLarge) }

// IsInsufficientCapacity reports whether err says the cloud had no capacity or quota for the requested offering.
func IsInsufficientCapacity(err error) bool { return hasReason(err, ReasonInsufficientCapacity) }

// IsQuotaExceeded reports whether err says a cloud quota refused the requested offering.
func IsQuotaExceeded(err error) bool {
	var apiErr *APIError
	return IsInsufficientCapacity(err) && errors.As(err, &apiErr) && apiErr.Cause == CauseQuota
}

// Client calls the plugin's VM-per-NodeClaim API.
type Client struct {
	baseURL string
	http    *http.Client
}

var _ API = (*Client)(nil)

// NewClient returns a client for the plugin at baseURL, such as http://plugin:4000; a nil httpClient uses
// http.DefaultClient.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

// Catalog implements API.
func (c *Client) Catalog(ctx context.Context, regions []string) (*Catalog, error) {
	q := url.Values{}
	if len(regions) > 0 {
		q.Set("regions", strings.Join(regions, ","))
	}
	var out Catalog
	if err := c.do(ctx, http.MethodGet, PathCatalog, q, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Create implements API.
func (c *Client) Create(ctx context.Context, req *CreateRequest) (*NodeClaim, error) {
	var out NodeClaim
	if err := c.do(ctx, http.MethodPost, PathNodeClaims, nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Get implements API.
func (c *Client) Get(ctx context.Context, name string) (*NodeClaim, error) {
	var out NodeClaim
	if err := c.do(ctx, http.MethodGet, PathNodeClaims+"/"+url.PathEscape(name), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetByProviderID implements API.
func (c *Client) GetByProviderID(ctx context.Context, providerID string) (*NodeClaim, error) {
	var out List
	if err := c.do(ctx, http.MethodGet, PathNodeClaims, url.Values{"providerID": {providerID}}, nil, &out); err != nil {
		return nil, err
	}
	if len(out.Items) == 0 {
		return nil, &APIError{Status: http.StatusNotFound, Reason: ReasonNotFound, Message: "no claim has providerID " + providerID}
	}
	return &out.Items[0], nil
}

// List implements API.
func (c *Client) List(ctx context.Context) ([]NodeClaim, error) {
	var out List
	if err := c.do(ctx, http.MethodGet, PathNodeClaims, nil, nil, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

// Delete implements API.
func (c *Client) Delete(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, PathNodeClaims+"/"+url.PathEscape(name), nil, nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	target := c.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		apiErr := &APIError{Status: resp.StatusCode}
		var parsed ErrorResponse
		if json.Unmarshal(raw, &parsed) == nil && parsed.Reason != "" {
			apiErr.Reason, apiErr.Cause, apiErr.Message = parsed.Reason, parsed.Cause, parsed.Message
		} else {
			apiErr.Reason, apiErr.Message = ReasonInternal, strings.TrimSpace(string(raw))
		}
		return apiErr
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s: %w", method, path, err)
	}
	return nil
}
