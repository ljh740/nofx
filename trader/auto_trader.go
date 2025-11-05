package trader

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"nofx/config"
	"nofx/decision"
	"nofx/logger"
	"nofx/market"
	"nofx/mcp"
	"nofx/news"
	"nofx/news/provider/telegram"
	"nofx/pool"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
)

// AutoTraderConfig 自动交易配置（简化版 - AI全权决策）
type AutoTraderConfig struct {
	// Trader标识
	ID      string // Trader唯一标识（用于日志目录等）
	Name    string // Trader显示名称
	AIModel string // AI模型: "qwen" 或 "deepseek"

	// 交易平台选择
	Exchange string // "binance", "hyperliquid" 或 "aster"

	// 币安API配置
	BinanceAPIKey    string
	BinanceSecretKey string

	// Hyperliquid配置
	HyperliquidPrivateKey string
	HyperliquidWalletAddr string
	HyperliquidTestnet    bool

	// Aster配置
	AsterUser       string // Aster主钱包地址
	AsterSigner     string // Aster API钱包地址
	AsterPrivateKey string // Aster API钱包私钥

	CoinPoolAPIURL string

	// AI配置
	UseQwen     bool
	DeepSeekKey string
	QwenKey     string

	// 自定义AI API配置
	CustomAPIURL    string
	CustomAPIKey    string
	CustomModelName string

	// 扫描配置
	ScanInterval time.Duration // 扫描间隔（建议3分钟）

	// 账户配置
	InitialBalance float64 // 初始金额（用于计算盈亏，需手动设置）

	// 杠杆配置
	BTCETHLeverage  int // BTC和ETH的杠杆倍数
	AltcoinLeverage int // 山寨币的杠杆倍数

	// 风险控制（仅作为提示，AI可自主决定）
	MaxDailyLoss    float64       // 最大日亏损百分比（提示）
	MaxDrawdown     float64       // 最大回撤百分比（提示）
	StopTradingTime time.Duration // 触发风控后暂停时长

	// 仓位模式
	IsCrossMargin bool // true=全仓模式, false=逐仓模式

	// 币种配置
	DefaultCoins []string // 默认币种列表（从数据库获取）
	TradingCoins []string // 实际交易币种列表

	// 系统提示词模板
	SystemPromptTemplate string // 系统提示词模板名称（如 "default", "aggressive"）

	// 新闻源配置
	NewsConfig []config.NewsConfig
}

// PositionSnapshot 持仓快照（用于检测自动平仓）
type PositionSnapshot struct {
	Symbol     string
	Side       string
	Quantity   float64
	EntryPrice float64
	Leverage   int
}

// TrendingCoinProvider 由可插拔模块实现，用于提供额外的热门交易币种。
type TrendingCoinProvider interface {
	TrendingCoins(traderID string, existing []string) []string
}

var (
	trendingProviderMu sync.RWMutex
	trendingProvider   TrendingCoinProvider
)

// RegisterTrendingCoinProvider 注册热门币种提供者，主流程可在启动时调用一次。
func RegisterTrendingCoinProvider(provider TrendingCoinProvider) {
	trendingProviderMu.Lock()
	defer trendingProviderMu.Unlock()
	trendingProvider = provider
}

func getTrendingCoins(traderID string, existing []string) []string {
	trendingProviderMu.RLock()
	provider := trendingProvider
	trendingProviderMu.RUnlock()
	if provider == nil {
		return nil
	}
	return provider.TrendingCoins(traderID, existing)
}

// AutoTrader 自动交易器
type AutoTrader struct {
	id                    string // Trader唯一标识
	name                  string // Trader显示名称
	aiModel               string // AI模型名称
	exchange              string // 交易平台名称
	config                AutoTraderConfig
	trader                Trader // 使用Trader接口（支持多平台）
	mcpClient             *mcp.Client
	decisionLogger        *logger.DecisionLogger // 决策日志记录器
	initialBalance        float64
	dailyPnL              float64
	customPrompt          string   // 自定义交易策略prompt
	overrideBasePrompt    bool     // 是否覆盖基础prompt
	systemPromptTemplate  string   // 系统提示词模板名称
	defaultCoins          []string // 默认币种列表（从数据库获取）
	tradingCoins          []string // 实际交易币种列表
	lastResetTime         time.Time
	stopUntil             time.Time
	startTime             time.Time                    // 系统启动时间
	callCount             int                          // AI调用次数
	positionFirstSeenTime map[string]int64             // 持仓首次出现时间 (symbol_side -> timestamp毫秒)
	lastPositions         map[string]*PositionSnapshot // 上一个周期的持仓快照 (symbol_side -> snapshot)
	newsProcessor         []news.Provider              // 新闻
	stateMu               sync.RWMutex                 // 保护运行状态
	isRunning             bool                         // 是否正在运行
	stopCh                chan struct{}                // 停止信号
	lastBalanceSyncTime   time.Time                    // 上次余额同步时间
	database              *config.Database             // 数据库引用（用于自动更新余额）
	userID                string                       // 用户ID
}

// NewAutoTrader 创建自动交易器
func NewAutoTrader(traderConfig AutoTraderConfig, db *config.Database, userID string) (*AutoTrader, error) {
	// 设置默认值
	if traderConfig.ID == "" {
		traderConfig.ID = "default_trader"
	}
	if traderConfig.Name == "" {
		traderConfig.Name = "Default Trader"
	}
	if traderConfig.AIModel == "" {
		if traderConfig.UseQwen {
			traderConfig.AIModel = "qwen"
		} else {
			traderConfig.AIModel = "deepseek"
		}
	}

	mcpClient := mcp.New()

	// 初始化AI
	if traderConfig.AIModel == "custom" {
		// 使用自定义API
		mcpClient.SetCustomAPI(traderConfig.CustomAPIURL, traderConfig.CustomAPIKey, traderConfig.CustomModelName)
		log.Printf("🤖 [%s] 使用自定义AI API: %s (模型: %s)", traderConfig.Name, traderConfig.CustomAPIURL, traderConfig.CustomModelName)
	} else if traderConfig.UseQwen || traderConfig.AIModel == "qwen" {
		// 使用Qwen (支持自定义URL和Model)
		mcpClient.SetQwenAPIKey(traderConfig.QwenKey, traderConfig.CustomAPIURL, traderConfig.CustomModelName)
		if traderConfig.CustomAPIURL != "" || traderConfig.CustomModelName != "" {
			log.Printf("🤖 [%s] 使用阿里云Qwen AI (自定义URL: %s, 模型: %s)", traderConfig.Name, traderConfig.CustomAPIURL, traderConfig.CustomModelName)
		} else {
			log.Printf("🤖 [%s] 使用阿里云Qwen AI", traderConfig.Name)
		}
	} else {
		// 默认使用DeepSeek (支持自定义URL和Model)
		mcpClient.SetDeepSeekAPIKey(traderConfig.DeepSeekKey, traderConfig.CustomAPIURL, traderConfig.CustomModelName)
		if traderConfig.CustomAPIURL != "" || traderConfig.CustomModelName != "" {
			log.Printf("🤖 [%s] 使用DeepSeek AI (自定义URL: %s, 模型: %s)", traderConfig.Name, traderConfig.CustomAPIURL, traderConfig.CustomModelName)
		} else {
			log.Printf("🤖 [%s] 使用DeepSeek AI", traderConfig.Name)
		}
	}

	// 初始化新闻提取器
	var newsProcessor []news.Provider
	for _, newsCfg := range traderConfig.NewsConfig {
		switch newsCfg.Provider {
		case news.ProviderTelegram:
			newsProvider, err := telegram.NewSearcher(newsCfg.Telegram.BaseURL,
				newsCfg.Telegram.ProxyURL,
				lo.Map(newsCfg.TelegramChannel, func(item config.NewsConfigTelegramChannel, _ int) telegram.Channel {
					return telegram.Channel{
						ID:   item.ID,
						Name: item.Name,
					}
				}),
			)
			if err != nil {
				panic(err)
			}

			newsProcessor = append(newsProcessor, newsProvider)
		}
	}

	// 初始化币种池API
	if traderConfig.CoinPoolAPIURL != "" {
		pool.SetCoinPoolAPI(traderConfig.CoinPoolAPIURL)
	}

	// 设置默认交易平台
	if traderConfig.Exchange == "" {
		traderConfig.Exchange = "binance"
	}

	// 根据配置创建对应的交易器
	var trader Trader
	var err error

	// 记录仓位模式（通用）
	marginModeStr := "全仓"
	if !traderConfig.IsCrossMargin {
		marginModeStr = "逐仓"
	}
	log.Printf("📊 [%s] 仓位模式: %s", traderConfig.Name, marginModeStr)

	switch traderConfig.Exchange {
	case "binance":
		log.Printf("🏦 [%s] 使用币安合约交易", traderConfig.Name)
		trader = NewFuturesTrader(traderConfig.BinanceAPIKey, traderConfig.BinanceSecretKey)
	case "hyperliquid":
		log.Printf("🏦 [%s] 使用Hyperliquid交易", traderConfig.Name)
		trader, err = NewHyperliquidTrader(traderConfig.HyperliquidPrivateKey, traderConfig.HyperliquidWalletAddr, traderConfig.HyperliquidTestnet)
		if err != nil {
			return nil, fmt.Errorf("初始化Hyperliquid交易器失败: %w", err)
		}
	case "aster":
		log.Printf("🏦 [%s] 使用Aster交易", traderConfig.Name)
		trader, err = NewAsterTrader(traderConfig.AsterUser, traderConfig.AsterSigner, traderConfig.AsterPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("初始化Aster交易器失败: %w", err)
		}
	default:
		return nil, fmt.Errorf("不支持的交易平台: %s", traderConfig.Exchange)
	}

	// 验证初始金额配置
	if traderConfig.InitialBalance <= 0 {
		return nil, fmt.Errorf("初始金额必须大于0，请在配置中设置InitialBalance")
	}

	// 初始化决策日志记录器（使用trader ID创建独立目录）
	logDir := fmt.Sprintf("decision_logs/%s", traderConfig.ID)
	decisionLogger := logger.NewDecisionLogger(logDir)

	// 设置默认系统提示词模板
	systemPromptTemplate := traderConfig.SystemPromptTemplate
	if systemPromptTemplate == "" {
		// feature/partial-close-dynamic-tpsl 分支默认使用 adaptive（支持动态止盈止损）
		systemPromptTemplate = "adaptive"
	}

	return &AutoTrader{
		id:                    traderConfig.ID,
		name:                  traderConfig.Name,
		aiModel:               traderConfig.AIModel,
		exchange:              traderConfig.Exchange,
		config:                traderConfig,
		trader:                trader,
		mcpClient:             mcpClient,
		decisionLogger:        decisionLogger,
		initialBalance:        traderConfig.InitialBalance,
		systemPromptTemplate:  systemPromptTemplate,
		defaultCoins:          traderConfig.DefaultCoins,
		tradingCoins:          traderConfig.TradingCoins,
		lastResetTime:         time.Now(),
		startTime:             time.Now(),
		callCount:             0,
		positionFirstSeenTime: make(map[string]int64),
		lastPositions:         make(map[string]*PositionSnapshot),
		newsProcessor:         newsProcessor,
		lastBalanceSyncTime:   time.Now(), // 初始化为当前时间
		database:              db,
		userID:                userID,
	}, nil
}

// Run 运行自动交易主循环
func (at *AutoTrader) Run() error {
	at.stateMu.Lock()
	if at.isRunning {
		at.stateMu.Unlock()
		return fmt.Errorf("trader %s 已在运行中", at.name)
	}

	at.isRunning = true
	at.stopCh = make(chan struct{})
	at.startTime = time.Now()
	stopCh := at.stopCh
	at.stateMu.Unlock()

	log.Println("🚀 AI驱动自动交易系统启动")
	log.Printf("💰 初始余额: %.2f USDT", at.initialBalance)
	log.Printf("⚙️  扫描间隔: %v", at.config.ScanInterval)
	log.Println("🤖 AI将全权决定杠杆、仓位大小、止损止盈等参数")

	ticker := time.NewTicker(at.config.ScanInterval)
	defer ticker.Stop()

	defer func() {
		at.stateMu.Lock()
		if at.stopCh == stopCh {
			at.stopCh = nil
		}
		at.isRunning = false
		at.stateMu.Unlock()
	}()

	// 首次立即执行
	if err := at.runCycle(); err != nil {
		log.Printf("❌ 执行失败: %v", err)
	}

	for {
		select {
		case <-stopCh:
			return nil
		case <-ticker.C:
			at.stateMu.RLock()
			running := at.isRunning
			at.stateMu.RUnlock()
			if !running {
				continue
			}

			if err := at.runCycle(); err != nil {
				log.Printf("❌ 执行失败: %v", err)
			}
		}
	}
}

// Stop 停止自动交易
func (at *AutoTrader) Stop() {
	at.stateMu.Lock()
	if !at.isRunning {
		at.stateMu.Unlock()
		return
	}

	at.isRunning = false
	if at.stopCh != nil {
		close(at.stopCh)
		at.stopCh = nil
	}
	at.stateMu.Unlock()

	log.Println("⏹ 自动交易系统停止")
}

func (at *AutoTrader) incrementCallCount() int {
	at.stateMu.Lock()
	at.callCount++
	count := at.callCount
	at.stateMu.Unlock()
	return count
}

// autoSyncBalanceIfNeeded 自动同步余额（智能检测充值/提现，即使有持仓也能检测）
func (at *AutoTrader) autoSyncBalanceIfNeeded() {
	// 距离上次同步不足10分钟，跳过
	if time.Since(at.lastBalanceSyncTime) < 10*time.Minute {
		return
	}

	log.Printf("🔄 [%s] 开始自动检查余额变化...", at.name)

	// 1. 查询实际余额
	balanceInfo, err := at.trader.GetBalance()
	if err != nil {
		log.Printf("⚠️ [%s] 查询余额失败: %v", at.name, err)
		at.lastBalanceSyncTime = time.Now()
		return
	}

	// 2. 提取总资产（总钱包余额）
	var totalBalance float64
	if total, ok := balanceInfo["total_wallet_balance"].(float64); ok && total > 0 {
		totalBalance = total
	} else if total, ok := balanceInfo["totalWalletBalance"].(float64); ok && total > 0 {
		totalBalance = total
	} else if total, ok := balanceInfo["balance"].(float64); ok && total > 0 {
		totalBalance = total
		log.Printf("⚠️ [%s] 使用 'balance' 字段作为总资产", at.name)
	} else {
		log.Printf("⚠️ [%s] 无法提取总资产", at.name)
		at.lastBalanceSyncTime = time.Now()
		return
	}

	// 3. 获取持仓信息并计算未实现盈亏
	positions, err := at.trader.GetPositions()
	if err != nil {
		log.Printf("⚠️ [%s] 获取持仓信息失败，为安全起见跳过余额同步: %v", at.name, err)
		at.lastBalanceSyncTime = time.Now()
		return
	}

	var totalUnrealizedPnl float64
	hasOpenPosition := false
	pnlFieldMissing := false

	for _, pos := range positions {
		if amt, ok := pos["positionAmt"].(float64); ok && math.Abs(amt) > 0.0001 {
			hasOpenPosition = true

			// 提取未实现盈亏（支持多种字段名）
			pnlFound := false
			if pnl, ok := pos["unrealizedProfit"].(float64); ok {
				totalUnrealizedPnl += pnl
				pnlFound = true
			} else if pnl, ok := pos["unRealizedProfit"].(float64); ok {
				totalUnrealizedPnl += pnl
				pnlFound = true
			} else if pnl, ok := pos["unRealizedProfit"].(string); ok {
				// 处理字符串类型的 PNL
				if parsedPnl, err := strconv.ParseFloat(pnl, 64); err == nil {
					totalUnrealizedPnl += parsedPnl
					pnlFound = true
				}
			}

			if !pnlFound {
				pnlFieldMissing = true
				posSymbol, _ := pos["symbol"].(string)
				log.Printf("  ⚠️ [%s] 持仓 %s 缺少未实现盈亏字段", at.name, posSymbol)
			}
		}
	}

	// 如果有持仓但无法获取盈亏数据，为安全起见跳过同步
	if hasOpenPosition && pnlFieldMissing {
		log.Printf("  ⚠️ [%s] 无法获取完整的未实现盈亏数据，跳过余额同步", at.name)
		at.lastBalanceSyncTime = time.Now()
		return
	}

	// 4. 计算净资产（总资产 - 未实现盈亏 = 实际投入本金）
	// 这个值不受持仓盈亏影响，只受充值/提现影响
	netBalance := totalBalance - totalUnrealizedPnl

	log.Printf("  [%s] 余额详情: 总资产=%.2f, 未实现盈亏=%.2f, 净资产=%.2f",
		at.name, totalBalance, totalUnrealizedPnl, netBalance)

	oldBalance := at.initialBalance

	// 防止除以零：如果初始余额无效，直接更新
	if oldBalance <= 0 {
		log.Printf("⚠️ [%s] 初始余额无效 (%.2f)，更新为当前净资产 %.2f USDT",
			at.name, oldBalance, netBalance)
		at.initialBalance = netBalance
		if at.database != nil {
			at.database.UpdateTraderInitialBalance(at.userID, at.id, netBalance)
		}
		at.lastBalanceSyncTime = time.Now()
		return
	}

	// 5. 计算净资产变化（这个变化排除了交易盈亏的影响）
	netChangeDiff := netBalance - oldBalance
	netChangePercent := (netChangeDiff / oldBalance) * 100

	// 6. 智能同步逻辑
	//    - 净资产变化超过 10%
	//    - 且净资产增加（排除提现和亏损）
	if math.Abs(netChangePercent) > 10.0 {
		if netBalance > oldBalance {
			// 净资产增加 → 很可能是充值
			log.Printf("🔔 [%s] 检测到净资产增加: %.2f → %.2f USDT (+%.2f, +%.2f%%)",
				at.name, oldBalance, netBalance, netChangeDiff, netChangePercent)

			if hasOpenPosition {
				log.Printf("  → 原因分析: 在有持仓的情况下净资产增加，很可能是用户充值")
			} else {
				log.Printf("  → 原因分析: 无持仓且净资产增加，可能是充值或交易盈利")
			}

			// 更新 initial_balance
			at.initialBalance = netBalance
			if at.database != nil {
				err := at.database.UpdateTraderInitialBalance(at.userID, at.id, netBalance)
				if err != nil {
					log.Printf("❌ [%s] 更新数据库失败: %v", at.name, err)
				} else {
					log.Printf("✅ [%s] 已自动同步余额到数据库", at.name)
				}
			}
		} else {
			// 净资产减少 → 可能是提现或亏损
			log.Printf("  ⚠️ [%s] 检测到净资产减少: %.2f → %.2f USDT (%.2f, %.2f%%)",
				at.name, oldBalance, netBalance, netChangeDiff, netChangePercent)

			if hasOpenPosition {
				log.Printf("  → 原因分析: 在有持仓的情况下净资产减少，可能是用户提现")
				log.Printf("  → 为保守起见，跳过同步（避免因交易亏损而错误更新）")
			} else {
				log.Printf("  → 原因分析: 无持仓且净资产减少，可能是提现或交易亏损")
				log.Printf("  → 跳过同步以保留原始 initial_balance，维持正确的盈亏基准")
			}
		}
	} else {
		log.Printf("  ✓ [%s] 净资产变化不大 (%.2f%%)，无需同步", at.name, netChangePercent)
	}

	at.lastBalanceSyncTime = time.Now()
}

// runCycle 运行一个交易周期（使用AI全权决策）
func (at *AutoTrader) runCycle() error {
	count := at.incrementCallCount()

	log.Print("\n" + strings.Repeat("=", 70))
	log.Printf("⏰ %s - AI决策周期 #%d", time.Now().Format("2006-01-02 15:04:05"), count)
	log.Print(strings.Repeat("=", 70))

	// 创建决策记录
	record := &logger.DecisionRecord{
		ExecutionLog: []string{},
		Success:      true,
	}

	// 1. 检查是否需要停止交易
	at.stateMu.RLock()
	stopUntil := at.stopUntil
	at.stateMu.RUnlock()

	if time.Now().Before(stopUntil) {
		remaining := stopUntil.Sub(time.Now())
		log.Printf("⏸ 风险控制：暂停交易中，剩余 %.0f 分钟", remaining.Minutes())
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("风险控制暂停中，剩余 %.0f 分钟", remaining.Minutes())
		at.decisionLogger.LogDecision(record)
		return nil
	}

	// 2. 重置日盈亏（每天重置）
	at.stateMu.Lock()
	if time.Since(at.lastResetTime) > 24*time.Hour {
		at.dailyPnL = 0
		at.lastResetTime = time.Now()
		log.Println("📅 日盈亏已重置")
	}
	at.stateMu.Unlock()

	// 3. 自动同步余额（每10分钟检查一次，充值/提现后自动更新）
	at.autoSyncBalanceIfNeeded()

	// 4. 收集交易上下文
	ctx, err := at.buildTradingContext(count)
	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("构建交易上下文失败: %v", err)
		at.decisionLogger.LogDecision(record)
		return fmt.Errorf("构建交易上下文失败: %w", err)
	}

	// 3.1 检测自动平仓（止损/止盈触发）
	autoClosedActions := at.detectAutoClosedPositions(ctx.Positions)
	for _, action := range autoClosedActions {
		log.Printf("[AUTO-CLOSE] 检测到自动平仓: %s %s (价格: %.4f)", action.Symbol, action.Action, action.Price)
		record.Decisions = append(record.Decisions, action)
		record.ExecutionLog = append(record.ExecutionLog,
			fmt.Sprintf("[AUTO-CLOSE] 自动平仓: %s %s (止损/止盈触发)", action.Symbol, action.Action))
	}

	// 保存账户状态快照
	record.AccountState = logger.AccountSnapshot{
		TotalBalance:          ctx.Account.TotalEquity,
		AvailableBalance:      ctx.Account.AvailableBalance,
		TotalUnrealizedProfit: ctx.Account.TotalPnL,
		PositionCount:         ctx.Account.PositionCount,
		MarginUsedPct:         ctx.Account.MarginUsedPct,
	}

	// 保存持仓快照
	for _, pos := range ctx.Positions {
		record.Positions = append(record.Positions, logger.PositionSnapshot{
			Symbol:           pos.Symbol,
			Side:             pos.Side,
			PositionAmt:      pos.Quantity,
			EntryPrice:       pos.EntryPrice,
			MarkPrice:        pos.MarkPrice,
			UnrealizedProfit: pos.UnrealizedPnL,
			Leverage:         float64(pos.Leverage),
			LiquidationPrice: pos.LiquidationPrice,
		})
	}

	// 保存候选币种列表
	for _, coin := range ctx.CandidateCoins {
		record.CandidateCoins = append(record.CandidateCoins, coin.Symbol)
	}

	log.Printf("📊 账户净值: %.2f USDT | 可用: %.2f USDT | 持仓: %d",
		ctx.Account.TotalEquity, ctx.Account.AvailableBalance, ctx.Account.PositionCount)

	// 4. 调用AI获取完整决策
	log.Printf("🤖 正在请求AI分析并决策... [模板: %s]", at.systemPromptTemplate)
	decision, err := decision.GetFullDecisionWithCustomPrompt(ctx, at.mcpClient, at.customPrompt, at.overrideBasePrompt, at.systemPromptTemplate)

	// 即使有错误，也保存思维链、决策和输入prompt（用于debug）
	if decision != nil {
		record.SystemPrompt = decision.SystemPrompt // 保存系统提示词
		record.InputPrompt = decision.UserPrompt
		record.CoTTrace = decision.CoTTrace
		if len(decision.Decisions) > 0 {
			decisionJSON, _ := json.MarshalIndent(decision.Decisions, "", "  ")
			record.DecisionJSON = string(decisionJSON)
		}
	}

	if err != nil {
		record.Success = false
		record.ErrorMessage = fmt.Sprintf("获取AI决策失败: %v", err)

		// 打印系统提示词和AI思维链（即使有错误，也要输出以便调试）
		if decision != nil {
			if decision.SystemPrompt != "" {
				log.Print("\n" + strings.Repeat("=", 70))
				log.Printf("📋 系统提示词 [模板: %s] (错误情况)", at.systemPromptTemplate)
				log.Println(strings.Repeat("=", 70))
				log.Println(decision.SystemPrompt)
				log.Print(strings.Repeat("=", 70) + "\n")
			}

			if decision.CoTTrace != "" {
				log.Print("\n" + strings.Repeat("-", 70))
				log.Println("💭 AI思维链分析（错误情况）:")
				log.Println(strings.Repeat("-", 70))
				log.Println(decision.CoTTrace)
				log.Print(strings.Repeat("-", 70) + "\n")
			}
		}

		at.decisionLogger.LogDecision(record)
		return fmt.Errorf("获取AI决策失败: %w", err)
	}

	// // 5. 打印系统提示词
	// log.Printf("\n" + strings.Repeat("=", 70))
	// log.Printf("📋 系统提示词 [模板: %s]", at.systemPromptTemplate)
	// log.Println(strings.Repeat("=", 70))
	// log.Println(decision.SystemPrompt)
	// log.Printf(strings.Repeat("=", 70) + "\n")

	// 6. 打印AI思维链
	// log.Printf("\n" + strings.Repeat("-", 70))
	// log.Println("💭 AI思维链分析:")
	// log.Println(strings.Repeat("-", 70))
	// log.Println(decision.CoTTrace)
	// log.Printf(strings.Repeat("-", 70) + "\n")

	// 7. 打印AI决策
	// log.Printf("📋 AI决策列表 (%d 个):\n", len(decision.Decisions))
	// for i, d := range decision.Decisions {
	// 	log.Printf("  [%d] %s: %s - %s", i+1, d.Symbol, d.Action, d.Reasoning)
	// 	if d.Action == "open_long" || d.Action == "open_short" {
	// 		log.Printf("      杠杆: %dx | 仓位: %.2f USDT | 止损: %.4f | 止盈: %.4f",
	// 			d.Leverage, d.PositionSizeUSD, d.StopLoss, d.TakeProfit)
	// 	}
	// }
	log.Println()

	// 8. 对决策排序：确保先平仓后开仓（防止仓位叠加超限）
	sortedDecisions := sortDecisionsByPriority(decision.Decisions)

	log.Println("🔄 执行顺序（已优化）: 先平仓→后开仓")
	for i, d := range sortedDecisions {
		log.Printf("  [%d] %s %s", i+1, d.Symbol, d.Action)
	}
	log.Println()

	// 执行决策并记录结果
	for _, d := range sortedDecisions {
		actionRecord := logger.DecisionAction{
			Action:    d.Action,
			Symbol:    d.Symbol,
			Quantity:  0,
			Leverage:  d.Leverage,
			Price:     0,
			Timestamp: time.Now(),
			Success:   false,
		}

		if err := at.executeDecisionWithRecord(&d, &actionRecord); err != nil {
			log.Printf("❌ 执行决策失败 (%s %s): %v", d.Symbol, d.Action, err)
			actionRecord.Error = err.Error()
			record.ExecutionLog = append(record.ExecutionLog, fmt.Sprintf("❌ %s %s 失败: %v", d.Symbol, d.Action, err))
		} else {
			actionRecord.Success = true
			record.ExecutionLog = append(record.ExecutionLog, fmt.Sprintf("✓ %s %s 成功", d.Symbol, d.Action))
			// 成功执行后短暂延迟
			time.Sleep(1 * time.Second)
		}

		record.Decisions = append(record.Decisions, actionRecord)
	}

	// 9. 更新持仓快照（用于下一周期检测自动平仓）
	// 注意：需要重新获取当前持仓，因为 AI 可能在本周期执行了平仓操作
	// ctx.Positions 是周期开始时的持仓，不反映本周期的变化
	currentPositionsAfterExecution, err := at.trader.GetPositions()
	if err != nil {
		log.Printf("⚠ 更新持仓快照失败，清空快照以避免误报: %v", err)
		at.lastPositions = make(map[string]*PositionSnapshot) // 清空快照防止误报
	} else {
		// 将原始持仓数据转换为快照格式
		snapshots := make([]PositionSnapshot, 0, len(currentPositionsAfterExecution))
		for _, pos := range currentPositionsAfterExecution {
			symbol := pos["symbol"].(string)
			side := pos["side"].(string)
			entryPrice := pos["entryPrice"].(float64)
			quantity := pos["positionAmt"].(float64)
			if quantity < 0 {
				quantity = -quantity // 空仓数量为负，转为正数
			}
			leverage := 10
			if lev, ok := pos["leverage"].(float64); ok {
				leverage = int(lev)
			}

			snapshots = append(snapshots, PositionSnapshot{
				Symbol:     symbol,
				Side:       side,
				EntryPrice: entryPrice,
				Quantity:   quantity,
				Leverage:   leverage,
			})
		}
		at.lastPositions = make(map[string]*PositionSnapshot)
		for _, snap := range snapshots {
			posKey := snap.Symbol + "_" + snap.Side
			// 创建副本避免指针问题
			snapshot := snap
			at.lastPositions[posKey] = &snapshot
		}
	}

	// 10. 保存决策记录
	if err := at.decisionLogger.LogDecision(record); err != nil {
		log.Printf("⚠ 保存决策记录失败: %v", err)
	}

	return nil
}

// buildTradingContext 构建交易上下文
func (at *AutoTrader) buildTradingContext(callCount int) (*decision.Context, error) {
	// 1. 获取账户信息
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("获取账户余额失败: %w", err)
	}

	// 获取账户字段
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Total Equity = 钱包余额 + 未实现盈亏
	totalEquity := totalWalletBalance + totalUnrealizedProfit

	// 2. 获取持仓信息
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	var positionInfos []decision.PositionInfo
	totalMarginUsed := 0.0

	// 当前持仓的key集合（用于清理已平仓的记录）
	currentPositionKeys := make(map[string]bool)

	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity // 空仓数量为负，转为正数
		}

		// 跳过已平仓的持仓（quantity = 0），防止"幽灵持仓"传递给AI
		if quantity == 0 {
			continue
		}

		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		// 计算占用保证金（估算）
		leverage := 10 // 默认值，实际应该从持仓信息获取
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}
		marginUsed := (quantity * markPrice) / float64(leverage)

		// 计算盈亏百分比（基于实际盈亏和保证金）
		pnlPct := 0.0
		if marginUsed > 0 {
			pnlPct = (unrealizedPnl / marginUsed) * 100
		}
		totalMarginUsed += marginUsed

		// 跟踪持仓首次出现时间
		posKey := symbol + "_" + side
		currentPositionKeys[posKey] = true
		if _, exists := at.positionFirstSeenTime[posKey]; !exists {
			// 新持仓，记录当前时间
			at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()
		}
		updateTime := at.positionFirstSeenTime[posKey]

		positionInfos = append(positionInfos, decision.PositionInfo{
			Symbol:           symbol,
			Side:             side,
			EntryPrice:       entryPrice,
			MarkPrice:        markPrice,
			Quantity:         quantity,
			Leverage:         leverage,
			UnrealizedPnL:    unrealizedPnl,
			UnrealizedPnLPct: pnlPct,
			LiquidationPrice: liquidationPrice,
			MarginUsed:       marginUsed,
			UpdateTime:       updateTime,
		})
	}

	// 清理已平仓的持仓记录，并撤销孤儿委托单
	for key := range at.positionFirstSeenTime {
		if !currentPositionKeys[key] {
			// 仓位消失了（可能被止损/止盈触发，或被強平）
			// 提取币种名称（key 格式：BTCUSDT_long 或 SOLUSDT_short）
			parts := strings.Split(key, "_")
			if len(parts) == 2 {
				symbol := parts[0]
				side := parts[1]
				log.Printf("⚠️ 检测到仓位消失: %s %s → 自动撤销委托单", symbol, side)

				// 撤销该币种的所有委托单（清理孤儿止损/止盈單）
				if err := at.trader.CancelAllOrders(symbol); err != nil {
					log.Printf("  ⚠️ 撤销 %s 委托单失败: %v", symbol, err)
				} else {
					log.Printf("  ✓ 已撤销 %s 的所有委托单", symbol)
				}
			}

			delete(at.positionFirstSeenTime, key)
		}
	}

	// 3. 获取交易员的候选币种池
	candidateCoins, err := at.getCandidateCoins()
	if err != nil {
		return nil, fmt.Errorf("获取候选币种失败: %w", err)
	}

	// 4. 计算总盈亏
	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	// 5. 分析历史表现（最近100个周期，避免长期持仓的交易记录丢失）
	// 假设每3分钟一个周期，100个周期 = 5小时，足够覆盖大部分交易
	performance, err := at.decisionLogger.AnalyzePerformance(100)
	if err != nil {
		log.Printf("⚠️  分析历史表现失败: %v", err)
		// 不影响主流程，继续执行（但设置performance为nil以避免传递错误数据）
		performance = nil
	}

	// 6. 提取新闻内容（根据持仓和候选币种动态收集）
	newsItem := make(map[string][]news.NewsItem)
	for _, newspro := range at.newsProcessor {
		// 收集需要新闻的币种（持仓 + 候选币前几个）
		newsSymbols := at.extractNewsSymbols(positionInfos, candidateCoins)

		if len(newsSymbols) == 0 {
			log.Printf("⚠️  没有需要收集新闻的币种，跳过新闻收集")
			continue
		}

		newsMap, err := newspro.FetchNews(newsSymbols, 100)
		if err != nil {
			log.Printf("⚠️  获取新闻内容失败: %v", err)
			continue
		}

		for symbol, value := range newsMap {
			newsItem[symbol] = append(newsItem[symbol], value...)
		}

		log.Printf("📰 收集了 %d 个币种的新闻: %v", len(newsSymbols), newsSymbols)
	}

	// 7. 构建上下文
	ctx := &decision.Context{
		CurrentTime:     time.Now().Format("2006-01-02 15:04:05"),
		RuntimeMinutes:  int(time.Since(at.startTime).Minutes()),
		CallCount:       callCount,
		BTCETHLeverage:  at.config.BTCETHLeverage,  // 使用配置的杠杆倍数
		AltcoinLeverage: at.config.AltcoinLeverage, // 使用配置的杠杆倍数
		Account: decision.AccountInfo{
			TotalEquity:      totalEquity,
			AvailableBalance: availableBalance,
			TotalPnL:         totalPnL,
			TotalPnLPct:      totalPnLPct,
			MarginUsed:       totalMarginUsed,
			MarginUsedPct:    marginUsedPct,
			PositionCount:    len(positionInfos),
		},
		Positions:      positionInfos,
		CandidateCoins: candidateCoins,
		Performance:    performance, // 添加历史表现分析
		News:           newsItem,
	}

	return ctx, nil
}

// executeDecisionWithRecord 执行AI决策并记录详细信息
func (at *AutoTrader) executeDecisionWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	switch decision.Action {
	case "open_long":
		return at.executeOpenLongWithRecord(decision, actionRecord)
	case "open_short":
		return at.executeOpenShortWithRecord(decision, actionRecord)
	case "close_long":
		return at.executeCloseLongWithRecord(decision, actionRecord)
	case "close_short":
		return at.executeCloseShortWithRecord(decision, actionRecord)
	case "update_stop_loss":
		return at.executeUpdateStopLossWithRecord(decision, actionRecord)
	case "update_take_profit":
		return at.executeUpdateTakeProfitWithRecord(decision, actionRecord)
	case "partial_close":
		return at.executePartialCloseWithRecord(decision, actionRecord)
	case "hold", "wait":
		// 无需执行，仅记录
		return nil
	default:
		return fmt.Errorf("未知的action: %s", decision.Action)
	}
}

// executeOpenLongWithRecord 执行开多仓并记录详细信息
func (at *AutoTrader) executeOpenLongWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📈 开多仓: %s", decision.Symbol)

	// ⚠️ 关键：检查是否已有同币种同方向持仓，如果有则拒绝开仓（防止仓位叠加超限）
	positions, err := at.trader.GetPositions()
	if err == nil {
		for _, pos := range positions {
			if pos["symbol"] == decision.Symbol && pos["side"] == "long" {
				return fmt.Errorf("❌ %s 已有多仓，拒绝开仓以防止仓位叠加超限。如需换仓，请先给出 close_long 决策", decision.Symbol)
			}
		}
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}

	// ⚠️ 保证金验证：防止保证金不足错误（code=-2019）
	// position_size_usd 是名义价值（包含杠杆），实际需要的保证金 = position_size_usd / leverage
	requiredMargin := decision.PositionSizeUSD / float64(decision.Leverage)

	// 获取当前可用余额
	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("获取账户余额失败: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// 手续费估算（Taker费率 0.04%）
	estimatedFee := decision.PositionSizeUSD * 0.0004
	totalRequired := requiredMargin + estimatedFee

	// 验证保证金充足（需要保证金 + 手续费 <= 可用余额）
	if totalRequired > availableBalance {
		return fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT。建议降低仓位或杠杆",
			totalRequired, requiredMargin, estimatedFee, availableBalance)
	}

	log.Printf("  ✓ 保证金检查通过: 需要 %.2f USDT，可用 %.2f USDT", totalRequired, availableBalance)

	// 计算数量
	quantity := decision.PositionSizeUSD / marketData.CurrentPrice
	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// 设置仓位模式
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 继续执行，不影响交易
	}

	// 开仓
	order, err := at.trader.OpenLong(decision.Symbol, quantity, decision.Leverage)
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 开仓成功，订单ID: %v, 数量: %.4f", order["orderId"], quantity)

	// 记录开仓时间
	posKey := decision.Symbol + "_long"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// 设置止损止盈
	if err := at.trader.SetStopLoss(decision.Symbol, "LONG", quantity, decision.StopLoss); err != nil {
		log.Printf("  ⚠ 设置止损失败: %v", err)
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "LONG", quantity, decision.TakeProfit); err != nil {
		log.Printf("  ⚠ 设置止盈失败: %v", err)
	}

	return nil
}

// executeOpenShortWithRecord 执行开空仓并记录详细信息
func (at *AutoTrader) executeOpenShortWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📉 开空仓: %s", decision.Symbol)

	// ⚠️ 关键：检查是否已有同币种同方向持仓，如果有则拒绝开仓（防止仓位叠加超限）
	positions, err := at.trader.GetPositions()
	if err == nil {
		for _, pos := range positions {
			if pos["symbol"] == decision.Symbol && pos["side"] == "short" {
				return fmt.Errorf("❌ %s 已有空仓，拒绝开仓以防止仓位叠加超限。如需换仓，请先给出 close_short 决策", decision.Symbol)
			}
		}
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}

	// ⚠️ 保证金验证：防止保证金不足错误（code=-2019）
	// position_size_usd 是名义价值（包含杠杆），实际需要的保证金 = position_size_usd / leverage
	requiredMargin := decision.PositionSizeUSD / float64(decision.Leverage)

	// 获取当前可用余额
	balance, err := at.trader.GetBalance()
	if err != nil {
		return fmt.Errorf("获取账户余额失败: %w", err)
	}
	availableBalance := 0.0
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// 手续费估算（Taker费率 0.04%）
	estimatedFee := decision.PositionSizeUSD * 0.0004
	totalRequired := requiredMargin + estimatedFee

	// 验证保证金充足（需要保证金 + 手续费 <= 可用余额）
	if totalRequired > availableBalance {
		return fmt.Errorf("❌ 保证金不足: 需要 %.2f USDT（保证金 %.2f + 手续费 %.2f），可用 %.2f USDT。建议降低仓位或杠杆",
			totalRequired, requiredMargin, estimatedFee, availableBalance)
	}

	log.Printf("  ✓ 保证金检查通过: 需要 %.2f USDT，可用 %.2f USDT", totalRequired, availableBalance)

	// 计算数量
	quantity := decision.PositionSizeUSD / marketData.CurrentPrice
	actionRecord.Quantity = quantity
	actionRecord.Price = marketData.CurrentPrice

	// 设置仓位模式
	if err := at.trader.SetMarginMode(decision.Symbol, at.config.IsCrossMargin); err != nil {
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 继续执行，不影响交易
	}

	// 开仓
	order, err := at.trader.OpenShort(decision.Symbol, quantity, decision.Leverage)
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 开仓成功，订单ID: %v, 数量: %.4f", order["orderId"], quantity)

	// 记录开仓时间
	posKey := decision.Symbol + "_short"
	at.positionFirstSeenTime[posKey] = time.Now().UnixMilli()

	// 设置止损止盈
	if err := at.trader.SetStopLoss(decision.Symbol, "SHORT", quantity, decision.StopLoss); err != nil {
		log.Printf("  ⚠ 设置止损失败: %v", err)
	}
	if err := at.trader.SetTakeProfit(decision.Symbol, "SHORT", quantity, decision.TakeProfit); err != nil {
		log.Printf("  ⚠ 设置止盈失败: %v", err)
	}

	return nil
}

// executeCloseLongWithRecord 执行平多仓并记录详细信息
func (at *AutoTrader) executeCloseLongWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🔄 平多仓: %s", decision.Symbol)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 平仓
	order, err := at.trader.CloseLong(decision.Symbol, 0) // 0 = 全部平仓
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 平仓成功")
	return nil
}

// executeCloseShortWithRecord 执行平空仓并记录详细信息
func (at *AutoTrader) executeCloseShortWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🔄 平空仓: %s", decision.Symbol)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 平仓
	order, err := at.trader.CloseShort(decision.Symbol, 0) // 0 = 全部平仓
	if err != nil {
		return err
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 平仓成功")
	return nil
}

// executeUpdateStopLossWithRecord 执行调整止损并记录详细信息
func (at *AutoTrader) executeUpdateStopLossWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🎯 调整止损: %s → %.2f", decision.Symbol, decision.NewStopLoss)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	if targetPosition == nil {
		return fmt.Errorf("持仓不存在: %s", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// 验证新止损价格合理性
	if positionSide == "LONG" && decision.NewStopLoss >= marketData.CurrentPrice {
		return fmt.Errorf("多单止损必须低于当前价格 (当前: %.2f, 新止损: %.2f)", marketData.CurrentPrice, decision.NewStopLoss)
	}
	if positionSide == "SHORT" && decision.NewStopLoss <= marketData.CurrentPrice {
		return fmt.Errorf("空单止损必须高于当前价格 (当前: %.2f, 新止损: %.2f)", marketData.CurrentPrice, decision.NewStopLoss)
	}

	// ⚠️ 防御性检查：检测是否存在双向持仓（不应该出现，但提供保护）
	var hasOppositePosition bool
	oppositeSide := ""
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posSide, _ := pos["side"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 && strings.ToUpper(posSide) != positionSide {
			hasOppositePosition = true
			oppositeSide = strings.ToUpper(posSide)
			break
		}
	}

	if hasOppositePosition {
		log.Printf("  🚨 警告：检测到 %s 存在双向持仓（%s + %s），这违反了策略规则",
			decision.Symbol, positionSide, oppositeSide)
		log.Printf("  🚨 取消止损单将影响两个方向的订单，请检查是否为用户手动操作导致")
		log.Printf("  🚨 建议：手动平掉其中一个方向的持仓，或检查系统是否有BUG")
	}

	// 取消旧的止损单（只删除止损单，不影响止盈单）
	// 注意：如果存在双向持仓，这会删除两个方向的止损单
	if err := at.trader.CancelStopLossOrders(decision.Symbol); err != nil {
		log.Printf("  ⚠ 取消旧止损单失败: %v", err)
		// 不中断执行，继续设置新止损
	}

	// 调用交易所 API 修改止损
	quantity := math.Abs(positionAmt)
	err = at.trader.SetStopLoss(decision.Symbol, positionSide, quantity, decision.NewStopLoss)
	if err != nil {
		return fmt.Errorf("修改止损失败: %w", err)
	}

	log.Printf("  ✓ 止损已调整: %.2f (当前价格: %.2f)", decision.NewStopLoss, marketData.CurrentPrice)
	return nil
}

// executeUpdateTakeProfitWithRecord 执行调整止盈并记录详细信息
func (at *AutoTrader) executeUpdateTakeProfitWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  🎯 调整止盈: %s → %.2f", decision.Symbol, decision.NewTakeProfit)

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	if targetPosition == nil {
		return fmt.Errorf("持仓不存在: %s", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// 验证新止盈价格合理性
	if positionSide == "LONG" && decision.NewTakeProfit <= marketData.CurrentPrice {
		return fmt.Errorf("多单止盈必须高于当前价格 (当前: %.2f, 新止盈: %.2f)", marketData.CurrentPrice, decision.NewTakeProfit)
	}
	if positionSide == "SHORT" && decision.NewTakeProfit >= marketData.CurrentPrice {
		return fmt.Errorf("空单止盈必须低于当前价格 (当前: %.2f, 新止盈: %.2f)", marketData.CurrentPrice, decision.NewTakeProfit)
	}

	// ⚠️ 防御性检查：检测是否存在双向持仓（不应该出现，但提供保护）
	var hasOppositePosition bool
	oppositeSide := ""
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posSide, _ := pos["side"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 && strings.ToUpper(posSide) != positionSide {
			hasOppositePosition = true
			oppositeSide = strings.ToUpper(posSide)
			break
		}
	}

	if hasOppositePosition {
		log.Printf("  🚨 警告：检测到 %s 存在双向持仓（%s + %s），这违反了策略规则",
			decision.Symbol, positionSide, oppositeSide)
		log.Printf("  🚨 取消止盈单将影响两个方向的订单，请检查是否为用户手动操作导致")
		log.Printf("  🚨 建议：手动平掉其中一个方向的持仓，或检查系统是否有BUG")
	}

	// 取消旧的止盈单（只删除止盈单，不影响止损单）
	// 注意：如果存在双向持仓，这会删除两个方向的止盈单
	if err := at.trader.CancelTakeProfitOrders(decision.Symbol); err != nil {
		log.Printf("  ⚠ 取消旧止盈单失败: %v", err)
		// 不中断执行，继续设置新止盈
	}

	// 调用交易所 API 修改止盈
	quantity := math.Abs(positionAmt)
	err = at.trader.SetTakeProfit(decision.Symbol, positionSide, quantity, decision.NewTakeProfit)
	if err != nil {
		return fmt.Errorf("修改止盈失败: %w", err)
	}

	log.Printf("  ✓ 止盈已调整: %.2f (当前价格: %.2f)", decision.NewTakeProfit, marketData.CurrentPrice)
	return nil
}

// executePartialCloseWithRecord 执行部分平仓并记录详细信息
func (at *AutoTrader) executePartialCloseWithRecord(decision *decision.Decision, actionRecord *logger.DecisionAction) error {
	log.Printf("  📊 部分平仓: %s %.1f%%", decision.Symbol, decision.ClosePercentage)

	// 验证百分比范围
	if decision.ClosePercentage <= 0 || decision.ClosePercentage > 100 {
		return fmt.Errorf("平仓百分比必须在 0-100 之间，当前: %.1f", decision.ClosePercentage)
	}

	// 获取当前价格
	marketData, err := market.Get(decision.Symbol)
	if err != nil {
		return err
	}
	actionRecord.Price = marketData.CurrentPrice

	// 获取当前持仓
	positions, err := at.trader.GetPositions()
	if err != nil {
		return fmt.Errorf("获取持仓失败: %w", err)
	}

	// 查找目标持仓
	var targetPosition map[string]interface{}
	for _, pos := range positions {
		symbol, _ := pos["symbol"].(string)
		posAmt, _ := pos["positionAmt"].(float64)
		if symbol == decision.Symbol && posAmt != 0 {
			targetPosition = pos
			break
		}
	}

	if targetPosition == nil {
		return fmt.Errorf("持仓不存在: %s", decision.Symbol)
	}

	// 获取持仓方向和数量
	side, _ := targetPosition["side"].(string)
	positionSide := strings.ToUpper(side)
	positionAmt, _ := targetPosition["positionAmt"].(float64)

	// 计算平仓数量
	totalQuantity := math.Abs(positionAmt)
	closeQuantity := totalQuantity * (decision.ClosePercentage / 100.0)
	actionRecord.Quantity = closeQuantity

	// ✅ Layer 2: 最小仓位检查（防止产生小额剩余）
	markPrice, _ := targetPosition["markPrice"].(float64)
	currentPositionValue := totalQuantity * markPrice
	remainingQuantity := totalQuantity - closeQuantity
	remainingValue := remainingQuantity * markPrice

	const MIN_POSITION_VALUE = 10.0 // 最小持仓价值 10 USDT

	if remainingValue > 0 && remainingValue < MIN_POSITION_VALUE {
		log.Printf("⚠️ 检测到 partial_close 后剩余仓位 %.2f USDT < %.0f USDT",
			remainingValue, MIN_POSITION_VALUE)
		log.Printf("  → 当前仓位价值: %.2f USDT, 平仓 %.1f%%, 剩余: %.2f USDT",
			currentPositionValue, decision.ClosePercentage, remainingValue)
		log.Printf("  → 自动修正为全部平仓，避免产生无法平仓的小额剩余")

		// 🔄 自动修正为全部平仓
		if positionSide == "LONG" {
			decision.Action = "close_long"
			log.Printf("  ✓ 已修正为: close_long")
			return at.executeCloseLongWithRecord(decision, actionRecord)
		} else {
			decision.Action = "close_short"
			log.Printf("  ✓ 已修正为: close_short")
			return at.executeCloseShortWithRecord(decision, actionRecord)
		}
	}

	// 执行平仓
	var order map[string]interface{}
	if positionSide == "LONG" {
		order, err = at.trader.CloseLong(decision.Symbol, closeQuantity)
	} else {
		order, err = at.trader.CloseShort(decision.Symbol, closeQuantity)
	}

	if err != nil {
		return fmt.Errorf("部分平仓失败: %w", err)
	}

	// 记录订单ID
	if orderID, ok := order["orderId"].(int64); ok {
		actionRecord.OrderID = orderID
	}

	log.Printf("  ✓ 部分平仓成功: 平仓 %.4f (%.1f%%), 剩余 %.4f",
		closeQuantity, decision.ClosePercentage, remainingQuantity)

	// 🔧 FIX: 部分平仓后重新设置止盈止损（基于剩余数量）
	// 币安会自动取消原来的止盈止损订单（因为数量不匹配），所以必须重新设置
	if decision.NewStopLoss > 0 || decision.NewTakeProfit > 0 {
		log.Printf("  🎯 更新剩余仓位的止盈止损...")

		// 设置新止损（基于剩余数量）
		if decision.NewStopLoss > 0 {
			if err := at.trader.SetStopLoss(decision.Symbol, positionSide, remainingQuantity, decision.NewStopLoss); err != nil {
				log.Printf("  ⚠️ 设置新止损失败: %v", err)
			} else {
				log.Printf("  ✓ 已设置新止损: %.4f (数量: %.4f)", decision.NewStopLoss, remainingQuantity)
			}
		}

		// 设置新止盈（基于剩余数量）
		if decision.NewTakeProfit > 0 {
			if err := at.trader.SetTakeProfit(decision.Symbol, positionSide, remainingQuantity, decision.NewTakeProfit); err != nil {
				log.Printf("  ⚠️ 设置新止盈失败: %v", err)
			} else {
				log.Printf("  ✓ 已设置新止盈: %.4f (数量: %.4f)", decision.NewTakeProfit, remainingQuantity)
			}
		}
	} else {
		// ⚠️ AI 没有提供新的止盈止损，剩余仓位将失去保护
		log.Printf("  ⚠️⚠️⚠️ 警告: 部分平仓后AI未提供新的止盈止损价格")
		log.Printf("  → 剩余仓位 %.4f (价值 %.2f USDT) 目前没有止盈止损保护", remainingQuantity, remainingValue)
		log.Printf("  → 建议: 在 partial_close 决策中包含 new_stop_loss 和 new_take_profit 字段")
	}

	return nil
}

// GetID 获取trader ID
func (at *AutoTrader) GetID() string {
	return at.id
}

// GetName 获取trader名称
func (at *AutoTrader) GetName() string {
	return at.name
}

// GetAIModel 获取AI模型
func (at *AutoTrader) GetAIModel() string {
	return at.aiModel
}

// GetExchange 获取交易所
func (at *AutoTrader) GetExchange() string {
	return at.exchange
}

// SetCustomPrompt 设置自定义交易策略prompt
func (at *AutoTrader) SetCustomPrompt(prompt string) {
	at.customPrompt = prompt
}

// SetOverrideBasePrompt 设置是否覆盖基础prompt
func (at *AutoTrader) SetOverrideBasePrompt(override bool) {
	at.overrideBasePrompt = override
}

// SetSystemPromptTemplate 设置系统提示词模板
func (at *AutoTrader) SetSystemPromptTemplate(templateName string) {
	at.systemPromptTemplate = templateName
}

// GetSystemPromptTemplate 获取当前系统提示词模板名称
func (at *AutoTrader) GetSystemPromptTemplate() string {
	return at.systemPromptTemplate
}

// GetDecisionLogger 获取决策日志记录器
func (at *AutoTrader) GetDecisionLogger() *logger.DecisionLogger {
	return at.decisionLogger
}

// GetStatus 获取系统状态（用于API）
func (at *AutoTrader) GetStatus() map[string]interface{} {
	aiProvider := "DeepSeek"
	if at.config.UseQwen {
		aiProvider = "Qwen"
	}

	at.stateMu.RLock()
	isRunning := at.isRunning
	startTime := at.startTime
	callCount := at.callCount
	stopUntil := at.stopUntil
	lastReset := at.lastResetTime
	dailyPnL := at.dailyPnL
	at.stateMu.RUnlock()

	return map[string]interface{}{
		"trader_id":       at.id,
		"trader_name":     at.name,
		"ai_model":        at.aiModel,
		"exchange":        at.exchange,
		"is_running":      isRunning,
		"start_time":      startTime.Format(time.RFC3339),
		"runtime_minutes": int(time.Since(startTime).Minutes()),
		"call_count":      callCount,
		"initial_balance": at.initialBalance,
		"scan_interval":   at.config.ScanInterval.String(),
		"stop_until":      stopUntil.Format(time.RFC3339),
		"last_reset_time": lastReset.Format(time.RFC3339),
		"daily_pnl":       dailyPnL,
		"ai_provider":     aiProvider,
	}
}

// GetAccountInfo 获取账户信息（用于API）
func (at *AutoTrader) GetAccountInfo() (map[string]interface{}, error) {
	balance, err := at.trader.GetBalance()
	if err != nil {
		return nil, fmt.Errorf("获取余额失败: %w", err)
	}

	// 获取账户字段
	totalWalletBalance := 0.0
	totalUnrealizedProfit := 0.0
	availableBalance := 0.0

	if wallet, ok := balance["totalWalletBalance"].(float64); ok {
		totalWalletBalance = wallet
	}
	if unrealized, ok := balance["totalUnrealizedProfit"].(float64); ok {
		totalUnrealizedProfit = unrealized
	}
	if avail, ok := balance["availableBalance"].(float64); ok {
		availableBalance = avail
	}

	// Total Equity = 钱包余额 + 未实现盈亏
	totalEquity := totalWalletBalance + totalUnrealizedProfit

	// 获取持仓计算总保证金
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	totalMarginUsed := 0.0
	totalUnrealizedPnL := 0.0
	for _, pos := range positions {
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		totalUnrealizedPnL += unrealizedPnl

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}
		marginUsed := (quantity * markPrice) / float64(leverage)
		totalMarginUsed += marginUsed
	}

	totalPnL := totalEquity - at.initialBalance
	totalPnLPct := 0.0
	if at.initialBalance > 0 {
		totalPnLPct = (totalPnL / at.initialBalance) * 100
	}

	marginUsedPct := 0.0
	if totalEquity > 0 {
		marginUsedPct = (totalMarginUsed / totalEquity) * 100
	}

	at.stateMu.RLock()
	dailyPnL := at.dailyPnL
	at.stateMu.RUnlock()

	return map[string]interface{}{
		// 核心字段
		"total_equity":      totalEquity,           // 账户净值 = wallet + unrealized
		"wallet_balance":    totalWalletBalance,    // 钱包余额（不含未实现盈亏）
		"unrealized_profit": totalUnrealizedProfit, // 未实现盈亏（从API）
		"available_balance": availableBalance,      // 可用余额

		// 盈亏统计
		"total_pnl":            totalPnL,           // 总盈亏 = equity - initial
		"total_pnl_pct":        totalPnLPct,        // 总盈亏百分比
		"total_unrealized_pnl": totalUnrealizedPnL, // 未实现盈亏（从持仓计算）
		"initial_balance":      at.initialBalance,  // 初始余额
		"daily_pnl":            dailyPnL,           // 日盈亏

		// 持仓信息
		"position_count":  len(positions),  // 持仓数量
		"margin_used":     totalMarginUsed, // 保证金占用
		"margin_used_pct": marginUsedPct,   // 保证金使用率
	}, nil
}

// GetPositions 获取持仓列表（用于API）
func (at *AutoTrader) GetPositions() ([]map[string]interface{}, error) {
	positions, err := at.trader.GetPositions()
	if err != nil {
		return nil, fmt.Errorf("获取持仓失败: %w", err)
	}

	var result []map[string]interface{}
	for _, pos := range positions {
		symbol := pos["symbol"].(string)
		side := pos["side"].(string)
		entryPrice := pos["entryPrice"].(float64)
		markPrice := pos["markPrice"].(float64)
		quantity := pos["positionAmt"].(float64)
		if quantity < 0 {
			quantity = -quantity
		}
		unrealizedPnl := pos["unRealizedProfit"].(float64)
		liquidationPrice := pos["liquidationPrice"].(float64)

		leverage := 10
		if lev, ok := pos["leverage"].(float64); ok {
			leverage = int(lev)
		}

		// 计算占用保证金
		marginUsed := (quantity * markPrice) / float64(leverage)

		// 计算盈亏百分比（基于保证金）
		// 收益率 = 未实现盈亏 / 保证金 × 100%
		pnlPct := 0.0
		if marginUsed > 0 {
			pnlPct = (unrealizedPnl / marginUsed) * 100
		}

		result = append(result, map[string]interface{}{
			"symbol":             symbol,
			"side":               side,
			"entry_price":        entryPrice,
			"mark_price":         markPrice,
			"quantity":           quantity,
			"leverage":           leverage,
			"unrealized_pnl":     unrealizedPnl,
			"unrealized_pnl_pct": pnlPct,
			"liquidation_price":  liquidationPrice,
			"margin_used":        marginUsed,
		})
	}

	return result, nil
}

// sortDecisionsByPriority 对决策排序：先平仓，再开仓，最后hold/wait
// 这样可以避免换仓时仓位叠加超限
func sortDecisionsByPriority(decisions []decision.Decision) []decision.Decision {
	if len(decisions) <= 1 {
		return decisions
	}

	// 定义优先级
	getActionPriority := func(action string) int {
		switch action {
		case "close_long", "close_short", "partial_close":
			return 1 // 最高优先级：先平仓（包括部分平仓）
		case "update_stop_loss", "update_take_profit":
			return 2 // 调整持仓止盈止损
		case "open_long", "open_short":
			return 3 // 次优先级：后开仓
		case "hold", "wait":
			return 4 // 最低优先级：观望
		default:
			return 999 // 未知动作放最后
		}
	}

	// 复制决策列表
	sorted := make([]decision.Decision, len(decisions))
	copy(sorted, decisions)

	// 按优先级排序
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if getActionPriority(sorted[i].Action) > getActionPriority(sorted[j].Action) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted
}

// getCandidateCoins 获取交易员的候选币种列表
func (at *AutoTrader) getCandidateCoins() ([]decision.CandidateCoin, error) {
	symbolSet := make(map[string]struct{})
	symbolsInOrder := make([]string, 0)
	var candidateCoins []decision.CandidateCoin

	addCandidate := func(symbol string, sources []string) {
		if symbol == "" {
			return
		}
		if _, exists := symbolSet[symbol]; exists {
			return
		}
		symbolSet[symbol] = struct{}{}
		symbolsInOrder = append(symbolsInOrder, symbol)
		candidateCoins = append(candidateCoins, decision.CandidateCoin{
			Symbol:  symbol,
			Sources: sources,
		})
	}

	if len(at.tradingCoins) == 0 {
		if len(at.defaultCoins) > 0 {
			for _, coin := range at.defaultCoins {
				addCandidate(normalizeSymbol(coin), []string{"default"})
			}
			log.Printf("📋 [%s] 使用数据库默认币种: %d个币种 %v",
				at.name, len(candidateCoins), at.defaultCoins)
		} else {
			const ai500Limit = 20
			mergedPool, err := pool.GetMergedCoinPool(ai500Limit)
			if err != nil {
				return nil, fmt.Errorf("获取合并币种池失败: %w", err)
			}
			for _, symbol := range mergedPool.AllSymbols {
				addCandidate(symbol, mergedPool.SymbolSources[symbol])
			}
			log.Printf("📋 [%s] 数据库无默认币种配置，使用AI500+OI Top: AI500前%d + OI_Top20 = 总计%d个候选币种",
				at.name, ai500Limit, len(candidateCoins))
		}
	} else {
		for _, coin := range at.tradingCoins {
			addCandidate(normalizeSymbol(coin), []string{"custom"})
		}
		log.Printf("📋 [%s] 使用自定义币种: %d个币种 %v",
			at.name, len(candidateCoins), at.tradingCoins)
	}

	if extra := getTrendingCoins(at.id, symbolsInOrder); len(extra) > 0 {
		appended := make([]string, 0, len(extra))
		for _, coin := range extra {
			symbol := normalizeSymbol(coin)
			before := len(candidateCoins)
			addCandidate(symbol, []string{"trending"})
			if len(candidateCoins) > before {
				appended = append(appended, symbol)
			}
		}
		if len(appended) > 0 {
			log.Printf("📈 [%s] 插入热门币种: %v", at.name, appended)
		}
	}

	return candidateCoins, nil
}

// normalizeSymbol 标准化币种符号（确保以USDT结尾）
func normalizeSymbol(symbol string) string {
	// 转为大写
	symbol = strings.ToUpper(strings.TrimSpace(symbol))

	// 确保以USDT结尾
	if !strings.HasSuffix(symbol, "USDT") {
		symbol = symbol + "USDT"
	}

	return symbol
}

// extractNewsSymbols 提取需要收集新闻的币种（持仓 + 候选币前几个 + BTC）
func (at *AutoTrader) extractNewsSymbols(positions []decision.PositionInfo, candidates []decision.CandidateCoin) []string {
	const (
		maxNewsSymbols       = 10 // 最多收集10个币种的新闻（避免请求过多）
		maxCandidatesForNews = 5  // 从候选币中取前5个
	)

	symbolSet := make(map[string]bool)
	result := make([]string, 0, maxNewsSymbols)

	// 1. 总是包含 BTC（市场风向标）
	symbolSet["btc"] = true
	result = append(result, "btc")

	// 2. 添加所有持仓币种（这些是最重要的）
	for _, pos := range positions {
		// 转换为新闻 API 格式（小写，移除 USDT 后缀）
		baseSymbol := strings.ToLower(strings.TrimSuffix(pos.Symbol, "USDT"))
		if !symbolSet[baseSymbol] && len(result) < maxNewsSymbols {
			symbolSet[baseSymbol] = true
			result = append(result, baseSymbol)
		}
	}

	// 3. 添加候选币种（前几个，按优先级）
	for i, coin := range candidates {
		if i >= maxCandidatesForNews {
			break
		}
		if len(result) >= maxNewsSymbols {
			break
		}

		baseSymbol := strings.ToLower(strings.TrimSuffix(coin.Symbol, "USDT"))
		if !symbolSet[baseSymbol] {
			symbolSet[baseSymbol] = true
			result = append(result, baseSymbol)
		}
	}

	return result
}

// detectAutoClosedPositions 检测自动平仓的持仓（止损/止盈触发）
func (at *AutoTrader) detectAutoClosedPositions(currentPositions []decision.PositionInfo) []logger.DecisionAction {
	var autoClosedActions []logger.DecisionAction

	// 创建当前持仓的map便于查找
	currentPosMap := make(map[string]bool)
	for _, pos := range currentPositions {
		posKey := pos.Symbol + "_" + pos.Side
		currentPosMap[posKey] = true
	}

	// 检查上一个周期的持仓，哪些现在消失了
	for posKey, lastPos := range at.lastPositions {
		if !currentPosMap[posKey] {
			// 这个持仓消失了，说明被自动平仓了（止损/止盈触发）
			// 获取当前价格作为平仓价格的近似值
			marketData, err := market.Get(lastPos.Symbol)
			closePrice := 0.0
			if err == nil {
				closePrice = marketData.CurrentPrice
			} else {
				// 如果无法获取当前价格，使用入场价作为fallback
				closePrice = lastPos.EntryPrice
			}

			// 确定是平多仓还是平空仓
			action := "auto_close_long"
			if lastPos.Side == "short" {
				action = "auto_close_short"
			}

			// 创建自动平仓记录
			autoClosedAction := logger.DecisionAction{
				Action:    action,
				Symbol:    lastPos.Symbol,
				Quantity:  lastPos.Quantity,
				Leverage:  lastPos.Leverage,
				Price:     closePrice,
				OrderID:   0, // 自动平仓没有特定的订单ID
				Timestamp: time.Now(),
				Success:   true,
				Error:     "",
			}

			autoClosedActions = append(autoClosedActions, autoClosedAction)
			log.Printf("[AUTO-CLOSE] 检测到自动平仓: %s %s @ %.4f (可能由止损/止盈触发)",
				lastPos.Symbol, action, closePrice)
		}
	}

	return autoClosedActions
}

// updatePositionSnapshots 更新持仓快照（用于下一周期检测自动平仓）
