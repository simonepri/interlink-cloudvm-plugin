// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/pricing"
)

type countingPricingClient struct {
	calls int
	price string
	err   error
}

func (c *countingPricingClient) GetProducts(_ context.Context, _ *pricing.GetProductsInput, _ ...func(*pricing.Options)) (*pricing.GetProductsOutput, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	item := fmt.Sprintf(`{"terms":{"OnDemand":{"T":{"priceDimensions":{"T.1":{"pricePerUnit":{"USD":%q}}}}}}}`, c.price)
	return &pricing.GetProductsOutput{PriceList: []string{item}}, nil
}

func TestOnDemandPriceBook_CachesListPrice(t *testing.T) {
	client := &countingPricingClient{price: "98.32"}
	book := NewOnDemandPriceBook(client)

	for range 3 {
		if got := book.Price(context.Background(), "us-east-1", "p5.48xlarge"); got != 98.32 {
			t.Fatalf("Price() = %v, want 98.32", got)
		}
	}
	if client.calls != 1 {
		t.Errorf("GetProducts called %d times, want 1", client.calls)
	}
}

func TestOnDemandPriceBook_UnknownShapeIsZero(t *testing.T) {
	book := NewOnDemandPriceBook(&countingPricingClient{err: fmt.Errorf("throttled")})

	if got := book.Price(context.Background(), "us-east-1", "p5.48xlarge"); got != 0 {
		t.Errorf("Price() = %v, want 0 for a shape neither the API nor the catalog knows", got)
	}
	if got := book.Price(context.Background(), "us-east-1", "g5.xlarge"); got != 1.006 {
		t.Errorf("Price() = %v, want catalog rate 1.006 when the API fails", got)
	}
}

func TestOnDemandPriceBook_RetriesFailedLookup(t *testing.T) {
	client := &countingPricingClient{err: fmt.Errorf("throttled")}
	book := NewOnDemandPriceBook(client)
	now := time.Now()
	book.now = func() time.Time { return now }

	book.Price(context.Background(), "us-east-1", "p5.48xlarge")
	book.Price(context.Background(), "us-east-1", "p5.48xlarge")
	if client.calls != 1 {
		t.Fatalf("GetProducts called %d times before the retry interval, want 1", client.calls)
	}

	client.err = nil
	client.price = "98.32"
	now = now.Add(onDemandPriceRetry + time.Second)
	if got := book.Price(context.Background(), "us-east-1", "p5.48xlarge"); got != 98.32 {
		t.Errorf("Price() after retry interval = %v, want 98.32", got)
	}
}
