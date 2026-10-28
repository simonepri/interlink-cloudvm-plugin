// AWS data transfer prices: loads per-GB list prices between launch regions, across zones, and to the internet from the
// Price List API and caches them per source region on the on-demand price schedule.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"
)

// TransferKind names the path data takes out of a region.
type TransferKind string

// Transfer kinds priced by TransferPriceBook.
const (
	TransferCrossRegion TransferKind = "cross_region"
	TransferCrossZone   TransferKind = "cross_zone"
	TransferInternet    TransferKind = "internet"
)

// TransferDestinationInternet is the destination of internet transfers.
const TransferDestinationInternet = "internet"

// awsTransferTypes maps each kind to the AWSDataTransfer transferType whose products price it.
var awsTransferTypes = []struct {
	kind         TransferKind
	transferType string
}{
	{TransferCrossRegion, "InterRegion Outbound"},
	{TransferCrossZone, "IntraRegion"},
	{TransferInternet, "AWS Outbound"},
}

// awsInternetLocation is the toLocation of AWS Outbound products that price transfer to the internet.
const awsInternetLocation = "External"

// TransferRoutePrice is the USD per GB price of moving data from a source region to a destination.
type TransferRoutePrice struct {
	Source      string
	Destination string
	Kind        TransferKind
	USDPerGB    float64
}

type transferRoute struct {
	destination string
	kind        TransferKind
}

type transferSourcePrices struct {
	prices  map[transferRoute]float64
	expires time.Time
}

// TransferPriceBook caches AWS data transfer prices out of each launch region, read from the Price List API.
type TransferPriceBook struct {
	mu       sync.Mutex
	client   AWSPricingAPI
	regions  []string
	sources  map[string]transferSourcePrices
	inflight map[string]chan struct{}
	now      func() time.Time
}

// GlobalTransferPrices is the process-wide transfer price book, priced between the regions the provider launches in.
var GlobalTransferPrices = NewTransferPriceBook(nil)

// NewTransferPriceBook creates a transfer price book that reads prices through client between regions.
func NewTransferPriceBook(client AWSPricingAPI, regions ...string) *TransferPriceBook {
	return &TransferPriceBook{
		client:   client,
		regions:  append([]string(nil), regions...),
		sources:  make(map[string]transferSourcePrices),
		inflight: make(map[string]chan struct{}),
		now:      time.Now,
	}
}

// SetClient replaces the Price List API client and forgets cached prices.
func (b *TransferPriceBook) SetClient(client AWSPricingAPI) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.client = client
	b.sources = make(map[string]transferSourcePrices)
}

// SetRegions replaces the regions priced as sources and cross-region destinations and forgets cached prices.
func (b *TransferPriceBook) SetRegions(regions []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.regions = append([]string(nil), regions...)
	b.sources = make(map[string]transferSourcePrices)
}

// Regions returns a copy of the priced regions.
func (b *TransferPriceBook) Regions() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.regions...)
}

// Refresh reloads the prices of every source region whose cached prices are missing or expired, waiting for the API.
func (b *TransferPriceBook) Refresh(ctx context.Context) {
	for _, src := range b.Regions() {
		b.refreshSource(ctx, src)
	}
}

func (b *TransferPriceBook) refreshSource(ctx context.Context, src string) {
	for {
		b.mu.Lock()
		if entry, ok := b.sources[src]; ok && b.now().Before(entry.expires) {
			b.mu.Unlock()
			return
		}
		if wait, busy := b.inflight[src]; busy {
			b.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return
			}
		}
		done := make(chan struct{})
		b.inflight[src] = done
		client, regions := b.client, append([]string(nil), b.regions...)
		b.mu.Unlock()
		b.lookup(ctx, client, src, regions, done)
		return
	}
}

// TransferPrice returns the USD per GB price of moving data out of src to dst, without waiting on the network. dst is
// another region for cross_region, src itself for cross_zone, and TransferDestinationInternet for internet. On a miss
// or an expired entry it refreshes src in the background.
func (b *TransferPriceBook) TransferPrice(src, dst string, kind TransferKind) (float64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.sources[src]
	if (!ok || !b.now().Before(entry.expires)) && b.isRegion(src) {
		if _, busy := b.inflight[src]; !busy {
			done := make(chan struct{})
			b.inflight[src] = done
			go b.lookup(context.Background(), b.client, src, append([]string(nil), b.regions...), done)
		}
	}
	price, found := entry.prices[transferRoute{destination: dst, kind: kind}]
	return price, found
}

// Prices returns every cached route price, ordered by source, kind, and destination.
func (b *TransferPriceBook) Prices() []TransferRoutePrice {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []TransferRoutePrice
	for src, entry := range b.sources {
		for route, price := range entry.prices {
			out = append(out, TransferRoutePrice{Source: src, Destination: route.destination, Kind: route.kind, USDPerGB: price})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Destination < out[j].Destination
	})
	return out
}

func (b *TransferPriceBook) isRegion(region string) bool {
	for _, r := range b.regions {
		if r == region {
			return true
		}
	}
	return false
}

// lookup reads every transfer kind out of src. When any read fails it keeps the previous prices and retries sooner.
func (b *TransferPriceBook) lookup(ctx context.Context, client AWSPricingAPI, src string, regions []string, done chan struct{}) {
	prices, err := listAWSTransferPrices(ctx, client, src, regions)
	b.mu.Lock()
	ttl := onDemandPriceTTL
	if err != nil {
		log.Printf("[cloudvm-ticker] Warning: reading AWS data transfer prices from %s failed: %v", sanitizeLog(src), err) //nolint:gosec // region sanitized
		prices = b.sources[src].prices
		ttl = onDemandPriceRetry
	}
	b.sources[src] = transferSourcePrices{prices: prices, expires: b.now().Add(ttl)}
	delete(b.inflight, src)
	b.mu.Unlock()
	close(done)
}

// errNoPricingClient reports a lookup without a Price List API client.
var errNoPricingClient = errors.New("pricing client not configured")

// listAWSTransferPrices reads the cross-region, cross-zone, and internet prices of data leaving src, keeping
// cross-region destinations that are in regions.
func listAWSTransferPrices(ctx context.Context, client AWSPricingAPI, src string, regions []string) (map[transferRoute]float64, error) {
	if client == nil {
		return nil, errNoPricingClient
	}
	destinations := make(map[string]bool, len(regions))
	for _, r := range regions {
		if r != src {
			destinations[r] = true
		}
	}
	prices := make(map[transferRoute]float64)
	for _, tt := range awsTransferTypes {
		products, err := listAWSTransferProducts(ctx, client, src, tt.transferType)
		if err != nil {
			return nil, err
		}
		for _, raw := range products {
			dst, price, ok := parseAWSTransferProduct(raw, src, tt.kind, destinations)
			if ok {
				prices[transferRoute{destination: dst, kind: tt.kind}] = price
			}
		}
	}
	return prices, nil
}

func listAWSTransferProducts(ctx context.Context, client AWSPricingAPI, src, transferType string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, onDemandLookupTimeout)
	defer cancel()
	paginator := pricing.NewGetProductsPaginator(client, &pricing.GetProductsInput{
		ServiceCode: aws.String("AWSDataTransfer"),
		Filters: []pricingtypes.Filter{
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("transferType"), Value: aws.String(transferType)},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("fromRegionCode"), Value: aws.String(src)},
			{Type: pricingtypes.FilterTypeTermMatch, Field: aws.String("fromLocationType"), Value: aws.String("AWS Region")},
		},
	})
	var products []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		products = append(products, page.PriceList...)
	}
	return products, nil
}

// parseAWSTransferProduct returns the destination and USD per GB price of one AWSDataTransfer product out of src,
// and false when the product prices another route or has no USD price.
func parseAWSTransferProduct(raw, src string, kind TransferKind, destinations map[string]bool) (string, float64, bool) {
	var product struct {
		Product struct {
			Attributes struct {
				ToRegionCode string `json:"toRegionCode"`
				ToLocation   string `json:"toLocation"`
			} `json:"attributes"`
		} `json:"product"`
		Terms struct {
			OnDemand map[string]struct {
				PriceDimensions map[string]awsPriceDimension `json:"priceDimensions"`
			} `json:"OnDemand"`
		} `json:"terms"`
	}
	if err := json.Unmarshal([]byte(raw), &product); err != nil {
		return "", 0, false
	}
	attrs := product.Product.Attributes
	var dst string
	switch kind {
	case TransferCrossRegion:
		if !destinations[attrs.ToRegionCode] {
			return "", 0, false
		}
		dst = attrs.ToRegionCode
	case TransferCrossZone:
		if attrs.ToRegionCode != src {
			return "", 0, false
		}
		dst = src
	case TransferInternet:
		if attrs.ToLocation != awsInternetLocation {
			return "", 0, false
		}
		dst = TransferDestinationInternet
	}
	var dims []awsPriceDimension
	for _, term := range product.Terms.OnDemand {
		for _, dim := range term.PriceDimensions {
			dims = append(dims, dim)
		}
	}
	price, ok := firstPaidTierUSD(dims)
	return dst, price, ok
}

type awsPriceDimension struct {
	BeginRange   string            `json:"beginRange"`
	PricePerUnit map[string]string `json:"pricePerUnit"`
}

// firstPaidTierUSD returns the USD price of the lowest volume tier that charges, skipping free allowances, or 0 when
// every tier is free. Dimensions without a USD price are ignored, and false means none had one.
func firstPaidTierUSD(dims []awsPriceDimension) (float64, bool) {
	type tier struct{ begin, price float64 }
	var tiers []tier
	for _, d := range dims {
		usd, ok := d.PricePerUnit["USD"]
		if !ok {
			continue
		}
		price, err := strconv.ParseFloat(usd, 64)
		if err != nil || price < 0 {
			continue
		}
		begin, err := strconv.ParseFloat(d.BeginRange, 64)
		if err != nil {
			begin = 0
		}
		tiers = append(tiers, tier{begin: begin, price: price})
	}
	if len(tiers) == 0 {
		return 0, false
	}
	sort.Slice(tiers, func(i, j int) bool { return tiers[i].begin < tiers[j].begin })
	for _, t := range tiers {
		if t.price > 0 {
			return t.price, true
		}
	}
	return 0, true
}
