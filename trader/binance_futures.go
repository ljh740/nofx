package trader

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
)

// FuturesTrader 币安合约交易器
type FuturesTrader struct {
	client *futures.Client

	// 余额缓存
	cachedBalance     map[string]interface{}
	balanceCacheTime  time.Time
	balanceCacheMutex sync.RWMutex

	// 持仓缓存
	cachedPositions     []map[string]interface{}
	positionsCacheTime  time.Time
	positionsCacheMutex sync.RWMutex

	// 缓存有效期（15秒）
	cacheDuration time.Duration

	// 服务器时间同步
	timeSyncMutex    sync.Mutex
	lastTimeSync     time.Time
	timeSyncInterval time.Duration
}

// NewFuturesTrader 创建合约交易器
func NewFuturesTrader(apiKey, secretKey string) *FuturesTrader {
	client := futures.NewClient(apiKey, secretKey)
	trader := &FuturesTrader{
		client:           client,
		cacheDuration:    15 * time.Second, // 15秒缓存
		timeSyncInterval: 30 * time.Second,
	}

	if err := trader.syncServerTime(context.Background(), true); err != nil {
		log.Printf("⚠️ 初始化同步币安服务器时间失败: %v", err)
	}

	// 设置双向持仓模式（Hedge Mode）
	// 这是必需的，因为代码中使用了 PositionSide (LONG/SHORT)
	if err := trader.setDualSidePosition(); err != nil {
		log.Printf("⚠️ 设置双向持仓模式失败: %v (如果已是双向模式则忽略此警告)", err)
	}

	return trader
}

// syncServerTime 同步本地与币安服务器的时间偏移
func (t *FuturesTrader) syncServerTime(ctx context.Context, force bool) error {
	t.timeSyncMutex.Lock()
	defer t.timeSyncMutex.Unlock()

	if !force && !t.lastTimeSync.IsZero() && time.Since(t.lastTimeSync) < t.timeSyncInterval {
		return nil
	}

	offset, err := t.client.NewSetServerTimeService().Do(ctx)
	if err != nil {
		return err
	}

	t.lastTimeSync = time.Now()
	drift := time.Duration(offset) * time.Millisecond
	log.Printf("✓ Binance服务器时间同步成功 (offset=%s)", drift)
	return nil
}

// callWithTimeSync 在调用需要签名的接口前后处理服务器时间同步，并在时间偏差错误时重试一次
func (t *FuturesTrader) callWithTimeSync(operation string, call func() error) error {
	ctx := context.Background()

	if err := t.syncServerTime(ctx, false); err != nil {
		log.Printf("⚠️ 同步Binance服务器时间失败（%s）: %v", operation, err)
	}

	err := call()
	if err == nil {
		return nil
	}

	var apiErr *common.APIError
	if errors.As(err, &apiErr) && apiErr.Code == -1021 {
		log.Printf("⚠️ Binance返回时间偏差错误（%s），尝试强制同步后重试: %s", operation, apiErr.Message)
		if syncErr := t.syncServerTime(ctx, true); syncErr != nil {
			log.Printf("❌ Binance服务器时间强制同步失败: %v", syncErr)
			return err
		}

		err = call()
	}

	return err
}

// GetBalance 获取账户余额（带缓存）
func (t *FuturesTrader) GetBalance() (map[string]interface{}, error) {
	// 先检查缓存是否有效
	t.balanceCacheMutex.RLock()
	if t.cachedBalance != nil && time.Since(t.balanceCacheTime) < t.cacheDuration {
		cacheAge := time.Since(t.balanceCacheTime)
		t.balanceCacheMutex.RUnlock()
		log.Printf("✓ 使用缓存的账户余额（缓存时间: %.1f秒前）", cacheAge.Seconds())
		return t.cachedBalance, nil
	}
	t.balanceCacheMutex.RUnlock()

	// 缓存过期或不存在，调用API
	log.Printf("🔄 缓存过期，正在调用币安API获取账户余额...")

	var account *futures.Account
	err := t.callWithTimeSync("获取账户信息", func() error {
		var innerErr error
		account, innerErr = t.client.NewGetAccountService().Do(context.Background())
		return innerErr
	})
	if err != nil {
		log.Printf("❌ 币安API调用失败: %v", err)
		return nil, fmt.Errorf("获取账户信息失败: %w", err)
	}

	result := make(map[string]interface{})
	result["totalWalletBalance"], _ = strconv.ParseFloat(account.TotalWalletBalance, 64)
	result["availableBalance"], _ = strconv.ParseFloat(account.AvailableBalance, 64)
	result["totalUnrealizedProfit"], _ = strconv.ParseFloat(account.TotalUnrealizedProfit, 64)

	log.Printf("✓ 币安API返回: 总余额=%s, 可用=%s, 未实现盈亏=%s",
		account.TotalWalletBalance,
		account.AvailableBalance,
		account.TotalUnrealizedProfit)

	// 更新缓存
	t.balanceCacheMutex.Lock()
	t.cachedBalance = result
	t.balanceCacheTime = time.Now()
	t.balanceCacheMutex.Unlock()

	return result, nil
}

// GetPositions 获取所有持仓（带缓存）
func (t *FuturesTrader) GetPositions() ([]map[string]interface{}, error) {
	// 先检查缓存是否有效
	t.positionsCacheMutex.RLock()
	if t.cachedPositions != nil && time.Since(t.positionsCacheTime) < t.cacheDuration {
		cacheAge := time.Since(t.positionsCacheTime)
		t.positionsCacheMutex.RUnlock()
		log.Printf("✓ 使用缓存的持仓信息（缓存时间: %.1f秒前）", cacheAge.Seconds())
		return t.cachedPositions, nil
	}
	t.positionsCacheMutex.RUnlock()

	// 缓存过期或不存在，调用API
	log.Printf("🔄 缓存过期，正在调用币安API获取持仓信息...")

	var positions []*futures.PositionRisk
	err := t.callWithTimeSync("获取持仓信息", func() error {
		var innerErr error
		positions, innerErr = t.client.NewGetPositionRiskService().Do(context.Background())
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	var result []map[string]interface{}
	for _, pos := range positions {
		posAmt, _ := strconv.ParseFloat(pos.PositionAmt, 64)
		if posAmt == 0 {
			continue // 跳过无持仓的
		}

		posMap := make(map[string]interface{})
		posMap["symbol"] = pos.Symbol
		posMap["positionAmt"], _ = strconv.ParseFloat(pos.PositionAmt, 64)
		posMap["entryPrice"], _ = strconv.ParseFloat(pos.EntryPrice, 64)
		posMap["markPrice"], _ = strconv.ParseFloat(pos.MarkPrice, 64)
		posMap["unRealizedProfit"], _ = strconv.ParseFloat(pos.UnRealizedProfit, 64)
		posMap["leverage"], _ = strconv.ParseFloat(pos.Leverage, 64)
		posMap["liquidationPrice"], _ = strconv.ParseFloat(pos.LiquidationPrice, 64)

		// 判断方向
		if posAmt > 0 {
			posMap["side"] = "long"
		} else {
			posMap["side"] = "short"
		}

		result = append(result, posMap)
	}

	// 更新缓存
	t.positionsCacheMutex.Lock()
	t.cachedPositions = result
	t.positionsCacheTime = time.Now()
	t.positionsCacheMutex.Unlock()

	return result, nil
}

// SetMarginMode 设置仓位模式
func (t *FuturesTrader) SetMarginMode(symbol string, isCrossMargin bool) error {
	var marginType futures.MarginType
	if isCrossMargin {
		marginType = futures.MarginTypeCrossed
	} else {
		marginType = futures.MarginTypeIsolated
	}

	// 尝试设置仓位模式
	err := t.callWithTimeSync("设置仓位模式", func() error {
		return t.client.NewChangeMarginTypeService().
			Symbol(symbol).
			MarginType(marginType).
			Do(context.Background())
	})

	marginModeStr := "全仓"
	if !isCrossMargin {
		marginModeStr = "逐仓"
	}

	if err != nil {
		// 如果错误信息包含"No need to change"，说明仓位模式已经是目标值
		if contains(err.Error(), "No need to change margin type") {
			log.Printf("  ✓ %s 仓位模式已是 %s", symbol, marginModeStr)
			return nil
		}
		// 如果有持仓，无法更改仓位模式，但不影响交易
		if contains(err.Error(), "Margin type cannot be changed if there exists position") {
			log.Printf("  ⚠️ %s 有持仓，无法更改仓位模式，继续使用当前模式", symbol)
			return nil
		}
		// 检测多资产模式（错误码 -4168）
		if contains(err.Error(), "Multi-Assets mode") || contains(err.Error(), "-4168") || contains(err.Error(), "4168") {
			log.Printf("  ⚠️ %s 检测到多资产模式，强制使用全仓模式", symbol)
			log.Printf("  💡 提示：如需使用逐仓模式，请在币安关闭多资产模式")
			return nil
		}
		// 检测统一账户 API（Portfolio Margin）
		if contains(err.Error(), "unified") || contains(err.Error(), "portfolio") || contains(err.Error(), "Portfolio") {
			log.Printf("  ❌ %s 检测到统一账户 API，无法进行合约交易", symbol)
			return fmt.Errorf("请使用「现货与合约交易」API 权限，不要使用「统一账户 API」")
		}
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 不返回错误，让交易继续
		return nil
	}

	log.Printf("  ✓ %s 仓位模式已设置为 %s", symbol, marginModeStr)
	return nil
}

// setDualSidePosition 设置双向持仓模式（初始化时调用）
func (t *FuturesTrader) setDualSidePosition() error {
	// 尝试设置双向持仓模式
	err := t.callWithTimeSync("设置双向持仓模式", func() error {
		return t.client.NewChangePositionModeService().
			DualSide(true). // true = 双向持仓（Hedge Mode）
			Do(context.Background())
	})

	if err != nil {
		// 如果错误信息包含"No need to change"，说明已经是双向持仓模式
		if contains(err.Error(), "No need to change position side") {
			log.Printf("  ✓ 账户已是双向持仓模式（Hedge Mode）")
			return nil
		}
		// 其他错误则返回（但在调用方不会中断初始化）
		return err
	}

	log.Printf("  ✓ 账户已切换为双向持仓模式（Hedge Mode）")
	log.Printf("  ℹ️  双向持仓模式允许同时持有多单和空单")
	return nil
}

// SetLeverage 设置杠杆（智能判断+冷却期）
func (t *FuturesTrader) SetLeverage(symbol string, leverage int) error {
	// 先尝试获取当前杠杆（从持仓信息）
	currentLeverage := 0
	positions, err := t.GetPositions()
	if err == nil {
		for _, pos := range positions {
			if pos["symbol"] == symbol {
				if lev, ok := pos["leverage"].(float64); ok {
					currentLeverage = int(lev)
					break
				}
			}
		}
	}

	// 如果当前杠杆已经是目标杠杆，跳过
	if currentLeverage == leverage && currentLeverage > 0 {
		log.Printf("  ✓ %s 杠杆已是 %dx，无需切换", symbol, leverage)
		return nil
	}

	// 切换杠杆
	err = t.callWithTimeSync("设置杠杆", func() error {
		_, innerErr := t.client.NewChangeLeverageService().
			Symbol(symbol).
			Leverage(leverage).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		// 如果错误信息包含"No need to change"，说明杠杆已经是目标值
		if contains(err.Error(), "No need to change") {
			log.Printf("  ✓ %s 杠杆已是 %dx", symbol, leverage)
			return nil
		}
		return fmt.Errorf("设置杠杆失败: %w", err)
	}

	log.Printf("  ✓ %s 杠杆已切换为 %dx", symbol, leverage)

	// 切换杠杆后等待5秒（避免冷却期错误）
	log.Printf("  ⏱ 等待5秒冷却期...")
	time.Sleep(5 * time.Second)

	return nil
}

// OpenLong 开多仓
func (t *FuturesTrader) OpenLong(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	// 先取消该币种的所有委托单（清理旧的止损止盈单）
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消旧委托单失败（可能没有委托单）: %v", err)
	}

	// 设置杠杆
	if err := t.SetLeverage(symbol, leverage); err != nil {
		return nil, err
	}

	// 注意：仓位模式应该由调用方（AutoTrader）在开仓前通过 SetMarginMode 设置

	// 格式化数量到正确精度
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// ✅ 检查格式化后的数量是否为 0（防止四舍五入导致的错误）
	quantityFloat, parseErr := strconv.ParseFloat(quantityStr, 64)
	if parseErr != nil || quantityFloat <= 0 {
		return nil, fmt.Errorf("开倉數量過小，格式化後為 0 (原始: %.8f → 格式化: %s)。建議增加開倉金額或選擇價格更低的幣種", quantity, quantityStr)
	}

	// ✅ 检查最小名义价值（Binance 要求至少 10 USDT）
	if err := t.CheckMinNotional(symbol, quantityFloat); err != nil {
		return nil, err
	}

	// 创建市价买入订单
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("开多仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeBuy).
			PositionSide(futures.PositionSideTypeLong).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return nil, fmt.Errorf("开多仓失败: %w", err)
	}

	log.Printf("✓ 开多仓成功: %s 数量: %s", symbol, quantityStr)
	log.Printf("  订单ID: %d", order.OrderID)

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = order.Status
	return result, nil
}

// OpenShort 开空仓
func (t *FuturesTrader) OpenShort(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	// 先取消该币种的所有委托单（清理旧的止损止盈单）
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消旧委托单失败（可能没有委托单）: %v", err)
	}

	// 设置杠杆
	if err := t.SetLeverage(symbol, leverage); err != nil {
		return nil, err
	}

	// 注意：仓位模式应该由调用方（AutoTrader）在开仓前通过 SetMarginMode 设置

	// 格式化数量到正确精度
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// ✅ 检查格式化后的数量是否为 0（防止四舍五入导致的错误）
	quantityFloat, parseErr := strconv.ParseFloat(quantityStr, 64)
	if parseErr != nil || quantityFloat <= 0 {
		return nil, fmt.Errorf("开倉數量過小，格式化後為 0 (原始: %.8f → 格式化: %s)。建議增加開倉金額或選擇價格更低的幣種", quantity, quantityStr)
	}

	// ✅ 检查最小名义价值（Binance 要求至少 10 USDT）
	if err := t.CheckMinNotional(symbol, quantityFloat); err != nil {
		return nil, err
	}

	// 创建市价卖出订单
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("开空仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeSell).
			PositionSide(futures.PositionSideTypeShort).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return nil, fmt.Errorf("开空仓失败: %w", err)
	}

	log.Printf("✓ 开空仓成功: %s 数量: %s", symbol, quantityStr)
	log.Printf("  订单ID: %d", order.OrderID)

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = order.Status
	return result, nil
}

// CloseLong 平多仓
func (t *FuturesTrader) CloseLong(symbol string, quantity float64) (map[string]interface{}, error) {
	// 如果数量为0，获取当前持仓数量
	if quantity == 0 {
		positions, err := t.GetPositions()
		if err != nil {
			return nil, err
		}

		for _, pos := range positions {
			if pos["symbol"] == symbol && pos["side"] == "long" {
				quantity = pos["positionAmt"].(float64)
				break
			}
		}

		if quantity == 0 {
			return nil, fmt.Errorf("没有找到 %s 的多仓", symbol)
		}
	}

	// 格式化数量
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// ✅ Layer 3: 检查是否满足最小金额要求（MIN_NOTIONAL）
	if err := t.CheckMinNotional(symbol, quantity); err != nil {
		log.Printf("⚠️ %s 剩余仓位过小: %v", symbol, err)

		// 🔄 尝试获取实际持仓数量并强制平仓
		positions, posErr := t.GetPositions()
		if posErr == nil {
			for _, pos := range positions {
				if pos["symbol"] == symbol && pos["side"] == "long" {
					actualQty := pos["positionAmt"].(float64)
					if actualQty > 0 {
						log.Printf("  → 检测到小额仓位，尝试强制市价全平...")
						// 不检查最小金额，直接尝试平仓
						return t.forceCloseLong(symbol, actualQty)
					}
				}
			}
		}

		// 如果实在无法平仓，返回跳过状态（不中断程序）
		log.Printf("  → 无法平仓小额仓位，建议手动处理或等待价格上涨")
		return map[string]interface{}{
			"status": "skipped_min_notional",
			"symbol": symbol,
			"error":  err.Error(),
		}, nil
	}

	// 创建市价卖出订单（平多）
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("平多仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeSell).
			PositionSide(futures.PositionSideTypeLong).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return nil, fmt.Errorf("平多仓失败: %w", err)
	}

	log.Printf("✓ 平多仓成功: %s 数量: %s", symbol, quantityStr)

	// 平仓后取消该币种的所有挂单（止损止盈单）
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", err)
	}

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = order.Status
	return result, nil
}

// CloseShort 平空仓
func (t *FuturesTrader) CloseShort(symbol string, quantity float64) (map[string]interface{}, error) {
	// 如果数量为0，获取当前持仓数量
	if quantity == 0 {
		positions, err := t.GetPositions()
		if err != nil {
			return nil, err
		}

		for _, pos := range positions {
			if pos["symbol"] == symbol && pos["side"] == "short" {
				quantity = -pos["positionAmt"].(float64) // 空仓数量是负的，取绝对值
				break
			}
		}

		if quantity == 0 {
			return nil, fmt.Errorf("没有找到 %s 的空仓", symbol)
		}
	}

	// 格式化数量
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// ✅ Layer 3: 检查是否满足最小金额要求（MIN_NOTIONAL）
	if err := t.CheckMinNotional(symbol, quantity); err != nil {
		log.Printf("⚠️ %s 剩余仓位过小: %v", symbol, err)

		// 🔄 尝试获取实际持仓数量并强制平仓
		positions, posErr := t.GetPositions()
		if posErr == nil {
			for _, pos := range positions {
				if pos["symbol"] == symbol && pos["side"] == "short" {
					actualQty := -pos["positionAmt"].(float64) // 空仓数量是负的，取绝对值
					if actualQty > 0 {
						log.Printf("  → 检测到小额仓位，尝试强制市价全平...")
						// 不检查最小金额，直接尝试平仓
						return t.forceCloseShort(symbol, actualQty)
					}
				}
			}
		}

		// 如果实在无法平仓，返回跳过状态（不中断程序）
		log.Printf("  → 无法平仓小额仓位，建议手动处理或等待价格上涨")
		return map[string]interface{}{
			"status": "skipped_min_notional",
			"symbol": symbol,
			"error":  err.Error(),
		}, nil
	}

	// 创建市价买入订单（平空）
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("平空仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeBuy).
			PositionSide(futures.PositionSideTypeShort).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return nil, fmt.Errorf("平空仓失败: %w", err)
	}

	log.Printf("✓ 平空仓成功: %s 数量: %s", symbol, quantityStr)

	// 平仓后取消该币种的所有挂单（止损止盈单）
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", err)
	}

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = order.Status
	return result, nil
}

// forceCloseLong 强制平多仓（忽略最小金额限制，用于清理小额剩余仓位）
func (t *FuturesTrader) forceCloseLong(symbol string, quantity float64) (map[string]interface{}, error) {
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 直接尝试市价平仓，不检查 MIN_NOTIONAL
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("强制平多仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeSell).
			PositionSide(futures.PositionSideTypeLong).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		// 如果还是失败，记录错误但不中断
		log.Printf("❌ 强制平多仓失败: %v (可能需要手动处理)", err)
		return map[string]interface{}{
			"status": "force_close_failed",
			"symbol": symbol,
			"error":  err.Error(),
		}, nil
	}

	log.Printf("✓ 强制平多仓成功: %s 数量: %s (小额仓位已清理)", symbol, quantityStr)

	// 取消挂单
	if cancelErr := t.CancelAllOrders(symbol); cancelErr != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", cancelErr)
	}

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = "force_closed"
	return result, nil
}

// forceCloseShort 强制平空仓（忽略最小金额限制，用于清理小额剩余仓位）
func (t *FuturesTrader) forceCloseShort(symbol string, quantity float64) (map[string]interface{}, error) {
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 直接尝试市价平仓，不检查 MIN_NOTIONAL
	var order *futures.CreateOrderResponse
	err = t.callWithTimeSync("强制平空仓", func() error {
		var innerErr error
		order, innerErr = t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(futures.SideTypeBuy).
			PositionSide(futures.PositionSideTypeShort).
			Type(futures.OrderTypeMarket).
			Quantity(quantityStr).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		// 如果还是失败，记录错误但不中断
		log.Printf("❌ 强制平空仓失败: %v (可能需要手动处理)", err)
		return map[string]interface{}{
			"status": "force_close_failed",
			"symbol": symbol,
			"error":  err.Error(),
		}, nil
	}

	log.Printf("✓ 强制平空仓成功: %s 数量: %s (小额仓位已清理)", symbol, quantityStr)

	// 取消挂单
	if cancelErr := t.CancelAllOrders(symbol); cancelErr != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", cancelErr)
	}

	result := make(map[string]interface{})
	result["orderId"] = order.OrderID
	result["symbol"] = order.Symbol
	result["status"] = "force_closed"
	return result, nil
}

// CancelAllOrders 取消该币种的所有挂单
func (t *FuturesTrader) CancelAllOrders(symbol string) error {
	err := t.callWithTimeSync("取消挂单", func() error {
		return t.client.NewCancelAllOpenOrdersService().
			Symbol(symbol).
			Do(context.Background())
	})

	if err != nil {
		return fmt.Errorf("取消挂单失败: %w", err)
	}

	log.Printf("  ✓ 已取消 %s 的所有挂单", symbol)
	return nil
}

// CancelStopOrders 取消该币种的止盈/止损单（用于调整止盈止损位置）
func (t *FuturesTrader) CancelStopOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	orders, err := t.client.NewListOpenOrdersService().
		Symbol(symbol).
		Do(context.Background())

	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	// 过滤出止盈止损单并取消
	canceledCount := 0
	for _, order := range orders {
		orderType := order.Type

		// 只取消止损和止盈订单
		if orderType == futures.OrderTypeStopMarket ||
			orderType == futures.OrderTypeTakeProfitMarket ||
			orderType == futures.OrderTypeStop ||
			orderType == futures.OrderTypeTakeProfit {

			_, err := t.client.NewCancelOrderService().
				Symbol(symbol).
				OrderID(order.OrderID).
				Do(context.Background())

			if err != nil {
				log.Printf("  ⚠ 取消订单 %d 失败: %v", order.OrderID, err)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消 %s 的止盈/止损单 (订单ID: %d, 类型: %s)",
				symbol, order.OrderID, orderType)
		}
	}

	if canceledCount == 0 {
		log.Printf("  ℹ %s 没有止盈/止损单需要取消", symbol)
	} else {
		log.Printf("  ✓ 已取消 %s 的 %d 个止盈/止损单", symbol, canceledCount)
	}

	return nil
}

// CancelStopLossOrders 仅取消止损单（不影响止盈单）
func (t *FuturesTrader) CancelStopLossOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	orders, err := t.client.NewListOpenOrdersService().
		Symbol(symbol).
		Do(context.Background())

	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	// 过滤出止损单并取消（取消所有方向的止损单，包括LONG和SHORT）
	canceledCount := 0
	var cancelErrors []error
	for _, order := range orders {
		orderType := order.Type

		// 只取消止损订单（不取消止盈订单）
		if orderType == futures.OrderTypeStopMarket || orderType == futures.OrderTypeStop {
			_, err := t.client.NewCancelOrderService().
				Symbol(symbol).
				OrderID(order.OrderID).
				Do(context.Background())

			if err != nil {
				wrappedErr := fmt.Errorf("订单ID %d: %w", order.OrderID, err)
				cancelErrors = append(cancelErrors, wrappedErr)
				log.Printf("  ⚠ 取消止损单失败: %v", wrappedErr)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消止损单 (订单ID: %d, 类型: %s, 方向: %s)", order.OrderID, orderType, order.PositionSide)
		}
	}

	if canceledCount == 0 && len(cancelErrors) == 0 {
		log.Printf("  ℹ %s 没有止损单需要取消", symbol)
	} else if canceledCount > 0 {
		log.Printf("  ✓ 已取消 %s 的 %d 个止损单", symbol, canceledCount)
	}

	// 如果所有取消都失败了，返回错误
	if len(cancelErrors) > 0 && canceledCount == 0 {
		return fmt.Errorf("取消止损单失败: %v", cancelErrors)
	}

	return nil
}

// CancelTakeProfitOrders 仅取消止盈单（不影响止损单）
func (t *FuturesTrader) CancelTakeProfitOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	orders, err := t.client.NewListOpenOrdersService().
		Symbol(symbol).
		Do(context.Background())

	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	// 过滤出止盈单并取消（取消所有方向的止盈单，包括LONG和SHORT）
	canceledCount := 0
	var cancelErrors []error
	for _, order := range orders {
		orderType := order.Type

		// 只取消止盈订单（不取消止损订单）
		if orderType == futures.OrderTypeTakeProfitMarket || orderType == futures.OrderTypeTakeProfit {
			_, err := t.client.NewCancelOrderService().
				Symbol(symbol).
				OrderID(order.OrderID).
				Do(context.Background())

			if err != nil {
				wrappedErr := fmt.Errorf("订单ID %d: %w", order.OrderID, err)
				cancelErrors = append(cancelErrors, wrappedErr)
				log.Printf("  ⚠ 取消止盈单失败: %v", wrappedErr)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消止盈单 (订单ID: %d, 类型: %s, 方向: %s)", order.OrderID, orderType, order.PositionSide)
		}
	}

	if canceledCount == 0 && len(cancelErrors) == 0 {
		log.Printf("  ℹ %s 没有止盈单需要取消", symbol)
	} else if canceledCount > 0 {
		log.Printf("  ✓ 已取消 %s 的 %d 个止盈单", symbol, canceledCount)
	}

	// 如果所有取消都失败了，返回错误
	if len(cancelErrors) > 0 && canceledCount == 0 {
		return fmt.Errorf("取消止盈单失败: %v", cancelErrors)
	}

	return nil
}

// GetMarketPrice 获取市场价格
func (t *FuturesTrader) GetMarketPrice(symbol string) (float64, error) {
	prices, err := t.client.NewListPricesService().Symbol(symbol).Do(context.Background())
	if err != nil {
		return 0, fmt.Errorf("获取价格失败: %w", err)
	}

	if len(prices) == 0 {
		return 0, fmt.Errorf("未找到价格")
	}

	price, err := strconv.ParseFloat(prices[0].Price, 64)
	if err != nil {
		return 0, err
	}

	return price, nil
}

// GetMinNotional 获取交易对的最小名义价值（MIN_NOTIONAL）
// 不同交易对有不同的最小值，这里使用保守的默认值
// 实际可以从 Binance API 的 exchangeInfo 获取精确值
func (t *FuturesTrader) GetMinNotional(symbol string) float64 {
	// 使用保守的默认值 10 USDT，确保订单能够通过交易所验证
	return 10.0
}

// CheckMinNotional 检查订单是否满足最小名义价值要求
func (t *FuturesTrader) CheckMinNotional(symbol string, quantity float64) error {
	price, err := t.GetMarketPrice(symbol)
	if err != nil {
		return fmt.Errorf("获取市价失败: %w", err)
	}

	notionalValue := quantity * price
	minNotional := t.GetMinNotional(symbol)

	if notionalValue < minNotional {
		return fmt.Errorf(
			"订单金额 %.2f USDT 低于最小要求 %.2f USDT (数量: %.4f, 价格: %.4f)",
			notionalValue, minNotional, quantity, price,
		)
	}

	return nil
}

// CalculatePositionSize 计算仓位大小
func (t *FuturesTrader) CalculatePositionSize(balance, riskPercent, price float64, leverage int) float64 {
	riskAmount := balance * (riskPercent / 100.0)
	positionValue := riskAmount * float64(leverage)
	quantity := positionValue / price
	return quantity
}

// SetStopLoss 设置止损单
func (t *FuturesTrader) SetStopLoss(symbol string, positionSide string, quantity, stopPrice float64) error {
	var side futures.SideType
	var posSide futures.PositionSideType

	if positionSide == "LONG" {
		side = futures.SideTypeSell
		posSide = futures.PositionSideTypeLong
	} else {
		side = futures.SideTypeBuy
		posSide = futures.PositionSideTypeShort
	}

	// 格式化数量
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return err
	}

	err = t.callWithTimeSync("设置止损", func() error {
		_, innerErr := t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(side).
			PositionSide(posSide).
			Type(futures.OrderTypeStopMarket).
			StopPrice(fmt.Sprintf("%.8f", stopPrice)).
			Quantity(quantityStr).
			WorkingType(futures.WorkingTypeContractPrice).
			ClosePosition(true).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return fmt.Errorf("设置止损失败: %w", err)
	}

	log.Printf("  止损价设置: %.4f", stopPrice)
	return nil
}

// SetTakeProfit 设置止盈单
func (t *FuturesTrader) SetTakeProfit(symbol string, positionSide string, quantity, takeProfitPrice float64) error {
	var side futures.SideType
	var posSide futures.PositionSideType

	if positionSide == "LONG" {
		side = futures.SideTypeSell
		posSide = futures.PositionSideTypeLong
	} else {
		side = futures.SideTypeBuy
		posSide = futures.PositionSideTypeShort
	}

	// 格式化数量
	quantityStr, err := t.FormatQuantity(symbol, quantity)
	if err != nil {
		return err
	}

	err = t.callWithTimeSync("设置止盈", func() error {
		_, innerErr := t.client.NewCreateOrderService().
			Symbol(symbol).
			Side(side).
			PositionSide(posSide).
			Type(futures.OrderTypeTakeProfitMarket).
			StopPrice(fmt.Sprintf("%.8f", takeProfitPrice)).
			Quantity(quantityStr).
			WorkingType(futures.WorkingTypeContractPrice).
			ClosePosition(true).
			Do(context.Background())
		return innerErr
	})

	if err != nil {
		return fmt.Errorf("设置止盈失败: %w", err)
	}

	log.Printf("  止盈价设置: %.4f", takeProfitPrice)
	return nil
}

// GetSymbolPrecision 获取交易对的数量精度
func (t *FuturesTrader) GetSymbolPrecision(symbol string) (int, error) {
	exchangeInfo, err := t.client.NewExchangeInfoService().Do(context.Background())
	if err != nil {
		return 0, fmt.Errorf("获取交易规则失败: %w", err)
	}

	for _, s := range exchangeInfo.Symbols {
		if s.Symbol == symbol {
			// 从LOT_SIZE filter获取精度
			for _, filter := range s.Filters {
				if filter["filterType"] == "LOT_SIZE" {
					stepSize := filter["stepSize"].(string)
					precision := calculatePrecision(stepSize)
					log.Printf("  %s 数量精度: %d (stepSize: %s)", symbol, precision, stepSize)
					return precision, nil
				}
			}
		}
	}

	log.Printf("  ⚠ %s 未找到精度信息，使用默认精度3", symbol)
	return 3, nil // 默认精度为3
}

// calculatePrecision 从stepSize计算精度
func calculatePrecision(stepSize string) int {
	// 去除尾部的0
	stepSize = trimTrailingZeros(stepSize)

	// 查找小数点
	dotIndex := -1
	for i := 0; i < len(stepSize); i++ {
		if stepSize[i] == '.' {
			dotIndex = i
			break
		}
	}

	// 如果没有小数点或小数点在最后，精度为0
	if dotIndex == -1 || dotIndex == len(stepSize)-1 {
		return 0
	}

	// 返回小数点后的位数
	return len(stepSize) - dotIndex - 1
}

// trimTrailingZeros 去除尾部的0
func trimTrailingZeros(s string) string {
	// 如果没有小数点，直接返回
	if !stringContains(s, ".") {
		return s
	}

	// 从后向前遍历，去除尾部的0
	for len(s) > 0 && s[len(s)-1] == '0' {
		s = s[:len(s)-1]
	}

	// 如果最后一位是小数点，也去掉
	if len(s) > 0 && s[len(s)-1] == '.' {
		s = s[:len(s)-1]
	}

	return s
}

// FormatQuantity 格式化数量到正确的精度
func (t *FuturesTrader) FormatQuantity(symbol string, quantity float64) (string, error) {
	precision, err := t.GetSymbolPrecision(symbol)
	if err != nil {
		// 如果获取失败，使用默认格式
		return fmt.Sprintf("%.3f", quantity), nil
	}

	format := fmt.Sprintf("%%.%df", precision)
	return fmt.Sprintf(format, quantity), nil
}

// 辅助函数
func contains(s, substr string) bool {
	return len(s) >= len(substr) && stringContains(s, substr)
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
