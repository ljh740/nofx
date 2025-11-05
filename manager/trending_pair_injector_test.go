package manager

import (
	"context"
	"reflect"
	"testing"
)

type stubTrendingFetcher struct {
	pairs *[]string
}

func (s *stubTrendingFetcher) FetchTopPairs(ctx context.Context, limit int) ([]string, error) {
	if s.pairs == nil {
		return nil, nil
	}
	snapshot := append([]string(nil), (*s.pairs)...)
	if limit > 0 && len(snapshot) > limit {
		snapshot = snapshot[:limit]
	}
	return snapshot, nil
}

func TestTrendingPairInjectorTrendingCoinsDedup(t *testing.T) {
	t.Parallel()

	pairs := []string{"solusdt", "AVAXUSDT", "BNBUSDT"}
	fetcher := &stubTrendingFetcher{pairs: &pairs}
	injector := NewTrendingPairInjector(fetcher)

	if err := injector.Update(context.Background()); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	base := []string{"BTCUSDT", "SOLUSDT"}
	got := injector.TrendingCoins("trader-1", base)
	want := []string{"AVAXUSDT"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected trending coins: got %v want %v", got, want)
	}
}

func TestTrendingPairInjectorRefresh(t *testing.T) {
	t.Parallel()

	pairs := []string{"SOLUSDT", "AVAXUSDT"}
	fetcher := &stubTrendingFetcher{pairs: &pairs}
	injector := NewTrendingPairInjector(fetcher)

	if err := injector.Update(context.Background()); err != nil {
		t.Fatalf("first update failed: %v", err)
	}

	pairs = []string{"XRPUSDT", "AVAXUSDT"}
	if err := injector.Update(context.Background()); err != nil {
		t.Fatalf("second update failed: %v", err)
	}

	got := injector.TrendingCoins("trader-1", nil)
	want := []string{"XRPUSDT", "AVAXUSDT"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected trending coins after refresh: got %v want %v", got, want)
	}
}
