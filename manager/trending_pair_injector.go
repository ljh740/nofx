package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"nofx/trader"
)

const (
	defaultBinanceTickerEndpoint = "https://fapi.binance.com/fapi/v1/ticker/24hr"
)

// TrendingPairFetcher 定义了获取热门交易对数据的能力，便于单元测试或替换实现。
type TrendingPairFetcher interface {
	FetchTopPairs(ctx context.Context, limit int) ([]string, error)
}

// BinanceTrendingFetcher 使用币安永续合约 24h 涨幅榜接口作为数据源。
type BinanceTrendingFetcher struct {
	Endpoint string
	Client   *http.Client
}

type binanceTicker struct {
	Symbol             string `json:"symbol"`
	PriceChangePercent string `json:"priceChangePercent"`
}

// NewBinanceTrendingFetcher 返回默认的币安热门交易对抓取器。
func NewBinanceTrendingFetcher() *BinanceTrendingFetcher {
	return &BinanceTrendingFetcher{
		Endpoint: defaultBinanceTickerEndpoint,
		Client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (f *BinanceTrendingFetcher) FetchTopPairs(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit %d", limit)
	}

	endpoint := f.Endpoint
	if endpoint == "" {
		endpoint = defaultBinanceTickerEndpoint
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("构建请求失败: %w", err)
	}

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求币安24小时涨幅榜失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, fmt.Errorf("币安返回状态码=%d, 响应=%s", resp.StatusCode, string(body))
	}

	var tickers []binanceTicker
	if err := json.NewDecoder(resp.Body).Decode(&tickers); err != nil {
		return nil, fmt.Errorf("解析币安涨幅榜失败: %w", err)
	}

	if len(tickers) == 0 {
		return nil, errors.New("币安涨幅榜返回为空")
	}

	type rankedTicker struct {
		symbol string
		change float64
	}

	ranked := make([]rankedTicker, 0, len(tickers))
	for _, ticker := range tickers {
		if !strings.HasSuffix(ticker.Symbol, "USDT") {
			continue
		}
		change, err := parsePercent(ticker.PriceChangePercent)
		if err != nil {
			continue
		}
		ranked = append(ranked, rankedTicker{symbol: ticker.Symbol, change: change})
	}

	if len(ranked) == 0 {
		return nil, errors.New("未找到USDT交易对的涨幅数据")
	}

	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].change == ranked[j].change {
			return ranked[i].symbol < ranked[j].symbol
		}
		return ranked[i].change > ranked[j].change
	})

	if limit > len(ranked) {
		limit = len(ranked)
	}

	symbols := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		if ranked[i].change <= 0 {
			break
		}
		symbols = append(symbols, ranked[i].symbol)
	}

	if len(symbols) == 0 {
		symbols = make([]string, 0, limit)
		for i := 0; i < limit; i++ {
			symbols = append(symbols, ranked[i].symbol)
		}
	}

	return symbols, nil
}

func parsePercent(v string) (float64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, errors.New("empty percent")
	}
	return strconv.ParseFloat(v, 64)
}

// TrendingPairInjector 仅缓存热门交易对，并通过 trader 包的回调接口提供给业务逻辑。
type TrendingPairInjector struct {
	fetcher   TrendingPairFetcher
	pairCount int
	interval  time.Duration

	mu    sync.RWMutex
	pairs []string
}

var _ trader.TrendingCoinProvider = (*TrendingPairInjector)(nil)

// NewTrendingPairInjector 构造一个实例，缺省间隔为 6 小时，热门数量为 2。
func NewTrendingPairInjector(fetcher TrendingPairFetcher) *TrendingPairInjector {
	if fetcher == nil {
		fetcher = NewBinanceTrendingFetcher()
	}
	return &TrendingPairInjector{
		fetcher:   fetcher,
		pairCount: 2,
		interval:  6 * time.Hour,
	}
}

// Start 在后台循环执行 Update，直至 context 取消。
func (t *TrendingPairInjector) Start(ctx context.Context) {
	go t.run(ctx)
}

func (t *TrendingPairInjector) run(ctx context.Context) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("⌛ 热门交易对服务停止")
			return
		case <-ticker.C:
			if err := t.Update(ctx); err != nil {
				log.Printf("⚠️  更新热门交易对失败: %v", err)
			}
		}
	}
}

// Update 拉取最新热门交易对并缓存到内存。
func (t *TrendingPairInjector) Update(ctx context.Context) error {
	pairs, err := t.fetcher.FetchTopPairs(ctx, t.pairCount)
	if err != nil {
		return err
	}

	normalized := make([]string, 0, len(pairs))
	seen := make(map[string]struct{})
	for _, pair := range pairs {
		symbol := normalizeTrendingSymbol(pair)
		if symbol == "" {
			continue
		}
		if _, exists := seen[symbol]; exists {
			continue
		}
		seen[symbol] = struct{}{}
		normalized = append(normalized, symbol)
	}

	t.mu.Lock()
	t.pairs = normalized
	t.mu.Unlock()

	if len(normalized) > 0 {
		log.Printf("📊 当前热门交易对: %v", normalized)
	} else {
		log.Printf("⚠️  热门交易对结果为空")
	}

	return nil
}

// TrendingCoins 实现 trader.TrendingCoinProvider，根据当前缓存返回需要附加的热门交易对。
func (t *TrendingPairInjector) TrendingCoins(_ string, existing []string) []string {
	existingSet := make(map[string]struct{}, len(existing))
	for _, symbol := range existing {
		normalized := normalizeTrendingSymbol(symbol)
		if normalized != "" {
			existingSet[normalized] = struct{}{}
		}
	}

	t.mu.RLock()
	pairs := append([]string(nil), t.pairs...)
	t.mu.RUnlock()

	result := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		if _, exists := existingSet[pair]; exists {
			continue
		}
		result = append(result, pair)
	}
	return result
}

func normalizeTrendingSymbol(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if s == "" {
		return ""
	}
	if !strings.HasSuffix(s, "USDT") {
		return ""
	}
	return s
}
