package api

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"nofx/auth"
	"nofx/config"
	"nofx/decision"
	"nofx/manager"
	"nofx/trader"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Server HTTP API服务器
type Server struct {
	router        *gin.Engine
	traderManager *manager.TraderManager
	database      *config.Database
	port          int
}

// NewServer 创建API服务器
func NewServer(traderManager *manager.TraderManager, database *config.Database, port int) *Server {
	// 设置为Release模式（减少日志输出）
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	// 启用CORS
	router.Use(corsMiddleware())

	s := &Server{
		router:        router,
		traderManager: traderManager,
		database:      database,
		port:          port,
	}

	// 设置路由
	s.setupRoutes()

	return s
}

// corsMiddleware CORS中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusOK)
			return
		}

		c.Next()
	}
}

// setupRoutes 设置路由
func (s *Server) setupRoutes() {
	// API路由组
	api := s.router.Group("/api")
	{
		// 健康检查
		api.Any("/health", s.handleHealth)

		// 认证相关路由（无需认证）
		api.POST("/register", s.handleRegister)
		api.POST("/login", s.handleLogin)
		api.POST("/verify-otp", s.handleVerifyOTP)
		api.POST("/complete-registration", s.handleCompleteRegistration)

		// 系统支持的模型和交易所（无需认证）
		api.GET("/supported-models", s.handleGetSupportedModels)
		api.GET("/supported-exchanges", s.handleGetSupportedExchanges)

		// 系统配置（无需认证）
		api.GET("/config", s.handleGetSystemConfig)

		// 系统提示词模板管理（无需认证）
		api.GET("/prompt-templates", s.handleGetPromptTemplates)
		api.GET("/prompt-templates/:name", s.handleGetPromptTemplate)

		// 需要认证的路由
		protected := api.Group("/", s.authMiddleware())
		{
			// AI交易员管理
			protected.GET("/traders", s.handleTraderList)
			protected.GET("/traders/:id/config", s.handleGetTraderConfig)
			protected.POST("/traders", s.handleCreateTrader)
			protected.PUT("/traders/:id", s.handleUpdateTrader)
			protected.DELETE("/traders/:id", s.handleDeleteTrader)
			protected.POST("/traders/:id/start", s.handleStartTrader)
			protected.POST("/traders/:id/stop", s.handleStopTrader)
			protected.PUT("/traders/:id/prompt", s.handleUpdateTraderPrompt)
			protected.POST("/traders/:id/sync-balance", s.handleSyncBalance)

			// AI模型配置
			protected.GET("/models", s.handleGetModelConfigs)
			protected.PUT("/models", s.handleUpdateModelConfigs)

			// 交易所配置
			protected.GET("/exchanges", s.handleGetExchangeConfigs)
			protected.PUT("/exchanges", s.handleUpdateExchangeConfigs)

			// 用户信号源配置
			protected.GET("/user/signal-sources", s.handleGetUserSignalSource)
			protected.POST("/user/signal-sources", s.handleSaveUserSignalSource)

			// 竞赛总览
			protected.GET("/competition", s.handleCompetition)

			// 指定trader的数据（使用query参数 ?trader_id=xxx）
			protected.GET("/status", s.handleStatus)
			protected.GET("/account", s.handleAccount)
			protected.GET("/positions", s.handlePositions)
			protected.GET("/decisions", s.handleDecisions)
			protected.GET("/decisions/latest", s.handleLatestDecisions)
			protected.GET("/statistics", s.handleStatistics)
			protected.GET("/equity-history", s.handleEquityHistory)
			protected.GET("/performance", s.handlePerformance)
		}
	}
}

// handleHealth 健康检查
func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"time":   c.Request.Context().Value("time"),
	})
}

// handleGetSystemConfig 获取系统配置（客户端需要知道的配置）
func (s *Server) handleGetSystemConfig(c *gin.Context) {
	// 获取默认币种
	defaultCoinsStr, _ := s.database.GetSystemConfig("default_coins")
	var defaultCoins []string
	if defaultCoinsStr != "" {
		json.Unmarshal([]byte(defaultCoinsStr), &defaultCoins)
	}
	if len(defaultCoins) == 0 {
		// 使用硬编码的默认币种
		defaultCoins = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "HYPEUSDT"}
	}

	// 获取杠杆配置
	btcEthLeverageStr, _ := s.database.GetSystemConfig("btc_eth_leverage")
	altcoinLeverageStr, _ := s.database.GetSystemConfig("altcoin_leverage")

	btcEthLeverage := 5
	if val, err := strconv.Atoi(btcEthLeverageStr); err == nil && val > 0 {
		btcEthLeverage = val
	}

	altcoinLeverage := 5
	if val, err := strconv.Atoi(altcoinLeverageStr); err == nil && val > 0 {
		altcoinLeverage = val
	}

	c.JSON(http.StatusOK, gin.H{
		"admin_mode":       auth.IsAdminMode(),
		"default_coins":    defaultCoins,
		"btc_eth_leverage": btcEthLeverage,
		"altcoin_leverage": altcoinLeverage,
	})
}

// getTraderFromQuery 从query参数获取trader
func (s *Server) getTraderFromQuery(c *gin.Context) (*manager.TraderManager, string, error) {
	userID := c.GetString("user_id")
	traderID := c.Query("trader_id")

	// 确保用户的交易员已加载到内存中
	err := s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 加载用户 %s 的交易员失败: %v", userID, err)
	}

	if traderID == "" {
		// 如果没有指定trader_id，返回该用户的第一个trader
		ids := s.traderManager.GetTraderIDs()
		if len(ids) == 0 {
			return nil, "", fmt.Errorf("没有可用的trader")
		}

		// 获取用户的交易员列表，优先返回用户自己的交易员
		userTraders, err := s.database.GetTraders(userID)
		if err == nil && len(userTraders) > 0 {
			traderID = userTraders[0].ID
		} else {
			traderID = ids[0]
		}
	}

	return s.traderManager, traderID, nil
}

// AI交易员管理相关结构体
type CreateTraderRequest struct {
	Name                 string  `json:"name" binding:"required"`
	AIModelID            string  `json:"ai_model_id" binding:"required"`
	ExchangeID           string  `json:"exchange_id" binding:"required"`
	InitialBalance       float64 `json:"initial_balance"`
	ScanIntervalMinutes  int     `json:"scan_interval_minutes"`
	BTCETHLeverage       int     `json:"btc_eth_leverage"`
	AltcoinLeverage      int     `json:"altcoin_leverage"`
	TradingSymbols       string  `json:"trading_symbols"`
	CustomPrompt         string  `json:"custom_prompt"`
	OverrideBasePrompt   bool    `json:"override_base_prompt"`
	SystemPromptTemplate string  `json:"system_prompt_template"` // 系统提示词模板名称
	IsCrossMargin        *bool   `json:"is_cross_margin"`        // 指针类型，nil表示使用默认值true
	UseCoinPool          bool    `json:"use_coin_pool"`
	UseOITop             bool    `json:"use_oi_top"`
}

type ModelConfig struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Provider     string `json:"provider"`
	Enabled      bool   `json:"enabled"`
	APIKey       string `json:"apiKey,omitempty"`
	CustomAPIURL string `json:"customApiUrl,omitempty"`
}

type ExchangeConfig struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type"` // "cex" or "dex"
	Enabled   bool   `json:"enabled"`
	APIKey    string `json:"apiKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	Testnet   bool   `json:"testnet,omitempty"`
}

type UpdateModelConfigRequest struct {
	Models map[string]struct {
		Enabled         bool   `json:"enabled"`
		APIKey          string `json:"api_key"`
		CustomAPIURL    string `json:"custom_api_url"`
		CustomModelName string `json:"custom_model_name"`
	} `json:"models"`
}

type UpdateExchangeConfigRequest struct {
	Exchanges map[string]struct {
		Enabled               bool   `json:"enabled"`
		APIKey                string `json:"api_key"`
		SecretKey             string `json:"secret_key"`
		Testnet               bool   `json:"testnet"`
		HyperliquidWalletAddr string `json:"hyperliquid_wallet_addr"`
		AsterUser             string `json:"aster_user"`
		AsterSigner           string `json:"aster_signer"`
		AsterPrivateKey       string `json:"aster_private_key"`
	} `json:"exchanges"`
}

// handleCreateTrader 创建新的AI交易员
func (s *Server) handleCreateTrader(c *gin.Context) {
	userID := c.GetString("user_id")
	var req CreateTraderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 校验杠杆值
	if req.BTCETHLeverage < 0 || req.BTCETHLeverage > 50 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "BTC/ETH杠杆必须在1-50倍之间"})
		return
	}
	if req.AltcoinLeverage < 0 || req.AltcoinLeverage > 20 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "山寨币杠杆必须在1-20倍之间"})
		return
	}

	// 校验交易币种格式
	if req.TradingSymbols != "" {
		symbols := strings.Split(req.TradingSymbols, ",")
		for _, symbol := range symbols {
			symbol = strings.TrimSpace(symbol)
			if symbol != "" && !strings.HasSuffix(strings.ToUpper(symbol), "USDT") {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("无效的币种格式: %s，必须以USDT结尾", symbol)})
				return
			}
		}
	}

	// 生成交易员ID
	traderID := fmt.Sprintf("%s_%s_%d", req.ExchangeID, req.AIModelID, time.Now().Unix())

	// 设置默认值
	isCrossMargin := true // 默认为全仓模式
	if req.IsCrossMargin != nil {
		isCrossMargin = *req.IsCrossMargin
	}

	// 设置杠杆默认值（从系统配置获取）
	btcEthLeverage := 5
	altcoinLeverage := 5
	if req.BTCETHLeverage > 0 {
		btcEthLeverage = req.BTCETHLeverage
	} else {
		// 从系统配置获取默认值
		if btcEthLeverageStr, _ := s.database.GetSystemConfig("btc_eth_leverage"); btcEthLeverageStr != "" {
			if val, err := strconv.Atoi(btcEthLeverageStr); err == nil && val > 0 {
				btcEthLeverage = val
			}
		}
	}
	if req.AltcoinLeverage > 0 {
		altcoinLeverage = req.AltcoinLeverage
	} else {
		// 从系统配置获取默认值
		if altcoinLeverageStr, _ := s.database.GetSystemConfig("altcoin_leverage"); altcoinLeverageStr != "" {
			if val, err := strconv.Atoi(altcoinLeverageStr); err == nil && val > 0 {
				altcoinLeverage = val
			}
		}
	}

	// 设置系统提示词模板默认值
	systemPromptTemplate := "default"
	if req.SystemPromptTemplate != "" {
		systemPromptTemplate = req.SystemPromptTemplate
	}

	// 设置扫描间隔默认值
	scanIntervalMinutes := req.ScanIntervalMinutes
	if scanIntervalMinutes <= 0 {
		scanIntervalMinutes = 3 // 默认3分钟
	}

	// ✨ 查询交易所实际余额，覆盖用户输入
	actualBalance := req.InitialBalance // 默认使用用户输入
	exchanges, err := s.database.GetExchanges(userID)
	if err != nil {
		log.Printf("⚠️ 获取交易所配置失败，使用用户输入的初始资金: %v", err)
	}

	// 查找匹配的交易所配置
	var exchangeCfg *config.ExchangeConfig
	for _, ex := range exchanges {
		if ex.ID == req.ExchangeID {
			exchangeCfg = ex
			break
		}
	}

	if exchangeCfg == nil {
		log.Printf("⚠️ 未找到交易所 %s 的配置，使用用户输入的初始资金", req.ExchangeID)
	} else if !exchangeCfg.Enabled {
		log.Printf("⚠️ 交易所 %s 未启用，使用用户输入的初始资金", req.ExchangeID)
	} else {
		// 根据交易所类型创建临时 trader 查询余额
		var tempTrader trader.Trader
		var createErr error

		switch req.ExchangeID {
		case "binance":
			tempTrader = trader.NewFuturesTrader(exchangeCfg.APIKey, exchangeCfg.SecretKey)
		case "hyperliquid":
			tempTrader, createErr = trader.NewHyperliquidTrader(
				exchangeCfg.APIKey, // private key
				exchangeCfg.HyperliquidWalletAddr,
				exchangeCfg.Testnet,
			)
		case "aster":
			tempTrader, createErr = trader.NewAsterTrader(
				exchangeCfg.AsterUser,
				exchangeCfg.AsterSigner,
				exchangeCfg.AsterPrivateKey,
			)
		default:
			log.Printf("⚠️ 不支持的交易所类型: %s，使用用户输入的初始资金", req.ExchangeID)
		}

		if createErr != nil {
			log.Printf("⚠️ 创建临时 trader 失败，使用用户输入的初始资金: %v", createErr)
		} else if tempTrader != nil {
			// 查询实际余额
			balanceInfo, balanceErr := tempTrader.GetBalance()
			if balanceErr != nil {
				log.Printf("⚠️ 查询交易所余额失败，使用用户输入的初始资金: %v", balanceErr)
			} else {
				// 提取可用余额
				if availableBalance, ok := balanceInfo["available_balance"].(float64); ok && availableBalance > 0 {
					actualBalance = availableBalance
					log.Printf("✓ 查询到交易所实际余额: %.2f USDT (用户输入: %.2f USDT)", actualBalance, req.InitialBalance)
				} else if totalBalance, ok := balanceInfo["balance"].(float64); ok && totalBalance > 0 {
					// 有些交易所可能只返回 balance 字段
					actualBalance = totalBalance
					log.Printf("✓ 查询到交易所实际余额: %.2f USDT (用户输入: %.2f USDT)", actualBalance, req.InitialBalance)
				} else {
					log.Printf("⚠️ 无法从余额信息中提取可用余额，使用用户输入的初始资金")
				}
			}
		}
	}

	// 创建交易员配置（数据库实体）
	trader := &config.TraderRecord{
		ID:                   traderID,
		UserID:               userID,
		Name:                 req.Name,
		AIModelID:            req.AIModelID,
		ExchangeID:           req.ExchangeID,
		InitialBalance:       actualBalance, // 使用实际查询的余额
		BTCETHLeverage:       btcEthLeverage,
		AltcoinLeverage:      altcoinLeverage,
		TradingSymbols:       req.TradingSymbols,
		UseCoinPool:          req.UseCoinPool,
		UseOITop:             req.UseOITop,
		CustomPrompt:         req.CustomPrompt,
		OverrideBasePrompt:   req.OverrideBasePrompt,
		SystemPromptTemplate: systemPromptTemplate,
		IsCrossMargin:        isCrossMargin,
		ScanIntervalMinutes:  scanIntervalMinutes,
		IsRunning:            false,
	}

	// 保存到数据库
	err = s.database.CreateTrader(trader)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("创建交易员失败: %v", err)})
		return
	}

	// 立即将新交易员加载到TraderManager中
	err = s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 加载用户交易员到内存失败: %v", err)
		// 这里不返回错误，因为交易员已经成功创建到数据库
	}

	log.Printf("✓ 创建交易员成功: %s (模型: %s, 交易所: %s)", req.Name, req.AIModelID, req.ExchangeID)

	c.JSON(http.StatusCreated, gin.H{
		"trader_id":   traderID,
		"trader_name": req.Name,
		"ai_model":    req.AIModelID,
		"is_running":  false,
	})
}

// UpdateTraderRequest 更新交易员请求
type UpdateTraderRequest struct {
	Name                 string  `json:"name" binding:"required"`
	AIModelID            string  `json:"ai_model_id" binding:"required"`
	ExchangeID           string  `json:"exchange_id" binding:"required"`
	InitialBalance       float64 `json:"initial_balance"`
	ScanIntervalMinutes  int     `json:"scan_interval_minutes"`
	BTCETHLeverage       int     `json:"btc_eth_leverage"`
	AltcoinLeverage      int     `json:"altcoin_leverage"`
	TradingSymbols       string  `json:"trading_symbols"`
	CustomPrompt         string  `json:"custom_prompt"`
	OverrideBasePrompt   bool    `json:"override_base_prompt"`
	SystemPromptTemplate string  `json:"system_prompt_template"`
	IsCrossMargin        *bool   `json:"is_cross_margin"`
}

// handleUpdateTrader 更新交易员配置
func (s *Server) handleUpdateTrader(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	var req UpdateTraderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 检查交易员是否存在且属于当前用户
	traders, err := s.database.GetTraders(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取交易员列表失败"})
		return
	}

	var existingTrader *config.TraderRecord
	for _, trader := range traders {
		if trader.ID == traderID {
			existingTrader = trader
			break
		}
	}

	if existingTrader == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "交易员不存在"})
		return
	}

	// 设置默认值
	isCrossMargin := existingTrader.IsCrossMargin // 保持原值
	if req.IsCrossMargin != nil {
		isCrossMargin = *req.IsCrossMargin
	}

	// 设置杠杆默认值
	btcEthLeverage := req.BTCETHLeverage
	altcoinLeverage := req.AltcoinLeverage
	if btcEthLeverage <= 0 {
		btcEthLeverage = existingTrader.BTCETHLeverage // 保持原值
	}
	if altcoinLeverage <= 0 {
		altcoinLeverage = existingTrader.AltcoinLeverage // 保持原值
	}

	// 设置扫描间隔，允许更新
	scanIntervalMinutes := req.ScanIntervalMinutes
	if scanIntervalMinutes <= 0 {
		scanIntervalMinutes = existingTrader.ScanIntervalMinutes // 保持原值
	}

	// 设置系统提示词模板，允许更新
	systemPromptTemplate := req.SystemPromptTemplate
	if systemPromptTemplate == "" {
		systemPromptTemplate = existingTrader.SystemPromptTemplate // 保持原值
	}

	// 🔒 保護 initial_balance 不被修改
	initialBalance := existingTrader.InitialBalance
	if req.InitialBalance > 0 {
		// 檢查是否嘗試修改初始餘額（允許 0.01 USDT 的誤差）
		diff := math.Abs(req.InitialBalance - existingTrader.InitialBalance)
		if diff > 0.01 {
			// 記錄警告日誌
			log.Printf("⚠️ BLOCKED: User %s attempted to modify initial_balance | Trader=%s | Original=%.2f | Requested=%.2f | Diff=%.2f",
				userID, traderID, existingTrader.InitialBalance, req.InitialBalance, diff)

			c.JSON(http.StatusBadRequest, gin.H{
				"error": "不允許修改初始餘額",
				"code":  "INITIAL_BALANCE_IMMUTABLE",
				"details": gin.H{
					"current_value":   existingTrader.InitialBalance,
					"requested_value": req.InitialBalance,
					"difference":      diff,
					"trader_id":       traderID,
					"trader_name":     existingTrader.Name,
					"created_at":      existingTrader.CreatedAt,
				},
				"message": fmt.Sprintf(
					"初始餘額是固定的基準值，創建後不可修改。\n\n"+
						"當前初始餘額: %.2f USDT\n"+
						"嘗試修改為: %.2f USDT\n\n"+
						"如確實需要修改，請聯繫管理員。",
					existingTrader.InitialBalance,
					req.InitialBalance,
				),
			})
			return
		}
	}

	// 更新交易员配置
	trader := &config.TraderRecord{
		ID:                   traderID,
		UserID:               userID,
		Name:                 req.Name,
		AIModelID:            req.AIModelID,
		ExchangeID:           req.ExchangeID,
		InitialBalance:       initialBalance,
		BTCETHLeverage:       btcEthLeverage,
		AltcoinLeverage:      altcoinLeverage,
		TradingSymbols:       req.TradingSymbols,
		CustomPrompt:         req.CustomPrompt,
		OverrideBasePrompt:   req.OverrideBasePrompt,
		SystemPromptTemplate: systemPromptTemplate,
		IsCrossMargin:        isCrossMargin,
		ScanIntervalMinutes:  scanIntervalMinutes,
		IsRunning:            existingTrader.IsRunning, // 保持原值
	}

	// 更新数据库
	err = s.database.UpdateTrader(trader)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("更新交易员失败: %v", err)})
		return
	}

	// 重新加载交易员到内存
	err = s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 重新加载用户交易员到内存失败: %v", err)
	}

	log.Printf("✓ 更新交易员成功: %s (模型: %s, 交易所: %s)", req.Name, req.AIModelID, req.ExchangeID)

	c.JSON(http.StatusOK, gin.H{
		"trader_id":   traderID,
		"trader_name": req.Name,
		"ai_model":    req.AIModelID,
		"message":     "交易员更新成功",
	})
}

// handleDeleteTrader 删除交易员
func (s *Server) handleDeleteTrader(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	// 从数据库删除
	err := s.database.DeleteTrader(userID, traderID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("删除交易员失败: %v", err)})
		return
	}

	// 如果交易员正在运行，先停止它
	if trader, err := s.traderManager.GetTrader(traderID); err == nil {
		status := trader.GetStatus()
		if isRunning, ok := status["is_running"].(bool); ok && isRunning {
			trader.Stop()
			log.Printf("⏹  已停止运行中的交易员: %s", traderID)
		}
	}

	if removed := s.traderManager.RemoveTrader(traderID); removed {
		log.Printf("🧼  已从内存管理器移除交易员: %s", traderID)
	}

	log.Printf("✓ 交易员已删除: %s", traderID)
	c.JSON(http.StatusOK, gin.H{"message": "交易员已删除"})
}

// handleStartTrader 启动交易员
func (s *Server) handleStartTrader(c *gin.Context) {
	traderID := c.Param("id")

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "交易员不存在"})
		return
	}

	// 检查交易员是否已经在运行
	status := trader.GetStatus()
	if isRunning, ok := status["is_running"].(bool); ok && isRunning {
		c.JSON(http.StatusBadRequest, gin.H{"error": "交易员已在运行中"})
		return
	}

	// 启动交易员
	go func() {
		log.Printf("▶️  启动交易员 %s (%s)", traderID, trader.GetName())
		if err := trader.Run(); err != nil {
			log.Printf("❌ 交易员 %s 运行错误: %v", trader.GetName(), err)
		}
	}()

	// 更新数据库中的运行状态
	userID := c.GetString("user_id")
	err = s.database.UpdateTraderStatus(userID, traderID, true)
	if err != nil {
		log.Printf("⚠️  更新交易员状态失败: %v", err)
	}

	log.Printf("✓ 交易员 %s 已启动", trader.GetName())
	c.JSON(http.StatusOK, gin.H{"message": "交易员已启动"})
}

// handleStopTrader 停止交易员
func (s *Server) handleStopTrader(c *gin.Context) {
	traderID := c.Param("id")

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "交易员不存在"})
		return
	}

	// 检查交易员是否正在运行
	status := trader.GetStatus()
	if isRunning, ok := status["is_running"].(bool); ok && !isRunning {
		c.JSON(http.StatusBadRequest, gin.H{"error": "交易员已停止"})
		return
	}

	// 停止交易员
	trader.Stop()

	// 更新数据库中的运行状态
	userID := c.GetString("user_id")
	err = s.database.UpdateTraderStatus(userID, traderID, false)
	if err != nil {
		log.Printf("⚠️  更新交易员状态失败: %v", err)
	}

	log.Printf("⏹  交易员 %s 已停止", trader.GetName())
	c.JSON(http.StatusOK, gin.H{"message": "交易员已停止"})
}

// handleUpdateTraderPrompt 更新交易员自定义Prompt
func (s *Server) handleUpdateTraderPrompt(c *gin.Context) {
	traderID := c.Param("id")
	userID := c.GetString("user_id")

	var req struct {
		CustomPrompt       string `json:"custom_prompt"`
		OverrideBasePrompt bool   `json:"override_base_prompt"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 更新数据库
	err := s.database.UpdateTraderCustomPrompt(userID, traderID, req.CustomPrompt, req.OverrideBasePrompt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("更新自定义prompt失败: %v", err)})
		return
	}

	// 如果trader在内存中，更新其custom prompt和override设置
	trader, err := s.traderManager.GetTrader(traderID)
	if err == nil {
		trader.SetCustomPrompt(req.CustomPrompt)
		trader.SetOverrideBasePrompt(req.OverrideBasePrompt)
		log.Printf("✓ 已更新交易员 %s 的自定义prompt (覆盖基础=%v)", trader.GetName(), req.OverrideBasePrompt)
	}

	c.JSON(http.StatusOK, gin.H{"message": "自定义prompt已更新"})
}

// handleSyncBalance 同步交易所余额到initial_balance（选项B：手动同步 + 选项C：智能检测）
func (s *Server) handleSyncBalance(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	log.Printf("🔄 用户 %s 请求同步交易员 %s 的余额", userID, traderID)

	// 从数据库获取交易员配置（包含交易所信息）
	traderConfig, _, exchangeCfg, err := s.database.GetTraderConfig(userID, traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "交易员不存在"})
		return
	}

	if exchangeCfg == nil || !exchangeCfg.Enabled {
		c.JSON(http.StatusBadRequest, gin.H{"error": "交易所未配置或未启用"})
		return
	}

	// 创建临时 trader 查询余额
	var tempTrader trader.Trader
	var createErr error

	switch traderConfig.ExchangeID {
	case "binance":
		tempTrader = trader.NewFuturesTrader(exchangeCfg.APIKey, exchangeCfg.SecretKey)
	case "hyperliquid":
		tempTrader, createErr = trader.NewHyperliquidTrader(
			exchangeCfg.APIKey,
			exchangeCfg.HyperliquidWalletAddr,
			exchangeCfg.Testnet,
		)
	case "aster":
		tempTrader, createErr = trader.NewAsterTrader(
			exchangeCfg.AsterUser,
			exchangeCfg.AsterSigner,
			exchangeCfg.AsterPrivateKey,
		)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "不支持的交易所类型"})
		return
	}

	if createErr != nil {
		log.Printf("⚠️ 创建临时 trader 失败: %v", createErr)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("连接交易所失败: %v", createErr)})
		return
	}

	// 查询实际余额
	balanceInfo, balanceErr := tempTrader.GetBalance()
	if balanceErr != nil {
		log.Printf("⚠️ 查询交易所余额失败: %v", balanceErr)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("查询余额失败: %v", balanceErr)})
		return
	}

	// 提取可用余额
	var actualBalance float64
	if availableBalance, ok := balanceInfo["available_balance"].(float64); ok && availableBalance > 0 {
		actualBalance = availableBalance
	} else if availableBalance, ok := balanceInfo["availableBalance"].(float64); ok && availableBalance > 0 {
		actualBalance = availableBalance
	} else if totalBalance, ok := balanceInfo["balance"].(float64); ok && totalBalance > 0 {
		actualBalance = totalBalance
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "无法获取可用余额"})
		return
	}

	oldBalance := traderConfig.InitialBalance

	// ✅ 选项C：智能检测余额变化
	changePercent := ((actualBalance - oldBalance) / oldBalance) * 100
	changeType := "增加"
	if changePercent < 0 {
		changeType = "减少"
	}

	log.Printf("✓ 查询到交易所实际余额: %.2f USDT (当前配置: %.2f USDT, 变化: %.2f%%)",
		actualBalance, oldBalance, changePercent)

	// 更新数据库中的 initial_balance
	err = s.database.UpdateTraderInitialBalance(userID, traderID, actualBalance)
	if err != nil {
		log.Printf("❌ 更新initial_balance失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新余额失败"})
		return
	}

	// 重新加载交易员到内存
	err = s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 重新加载用户交易员到内存失败: %v", err)
	}

	log.Printf("✅ 已同步余额: %.2f → %.2f USDT (%s %.2f%%)", oldBalance, actualBalance, changeType, changePercent)

	c.JSON(http.StatusOK, gin.H{
		"message":        "余额同步成功",
		"old_balance":    oldBalance,
		"new_balance":    actualBalance,
		"change_percent": changePercent,
		"change_type":    changeType,
	})
}

// handleGetModelConfigs 获取AI模型配置
func (s *Server) handleGetModelConfigs(c *gin.Context) {
	userID := c.GetString("user_id")
	log.Printf("🔍 查询用户 %s 的AI模型配置", userID)
	models, err := s.database.GetAIModels(userID)
	if err != nil {
		log.Printf("❌ 获取AI模型配置失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("获取AI模型配置失败: %v", err)})
		return
	}
	log.Printf("✅ 找到 %d 个AI模型配置", len(models))

	c.JSON(http.StatusOK, models)
}

// handleUpdateModelConfigs 更新AI模型配置
func (s *Server) handleUpdateModelConfigs(c *gin.Context) {
	userID := c.GetString("user_id")
	var req UpdateModelConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 更新每个模型的配置
	for modelID, modelData := range req.Models {
		err := s.database.UpdateAIModel(userID, modelID, modelData.Enabled, modelData.APIKey, modelData.CustomAPIURL, modelData.CustomModelName)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("更新模型 %s 失败: %v", modelID, err)})
			return
		}
	}

	// 重新加载该用户的所有交易员，使新配置立即生效
	err := s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 重新加载用户交易员到内存失败: %v", err)
		// 这里不返回错误，因为模型配置已经成功更新到数据库
	}

	log.Printf("✓ AI模型配置已更新: %+v", req.Models)
	c.JSON(http.StatusOK, gin.H{"message": "模型配置已更新"})
}

// handleGetExchangeConfigs 获取交易所配置
func (s *Server) handleGetExchangeConfigs(c *gin.Context) {
	userID := c.GetString("user_id")
	log.Printf("🔍 查询用户 %s 的交易所配置", userID)
	exchanges, err := s.database.GetExchanges(userID)
	if err != nil {
		log.Printf("❌ 获取交易所配置失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("获取交易所配置失败: %v", err)})
		return
	}
	log.Printf("✅ 找到 %d 个交易所配置", len(exchanges))

	c.JSON(http.StatusOK, exchanges)
}

// handleUpdateExchangeConfigs 更新交易所配置
func (s *Server) handleUpdateExchangeConfigs(c *gin.Context) {
	userID := c.GetString("user_id")
	var req UpdateExchangeConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 更新每个交易所的配置
	for exchangeID, exchangeData := range req.Exchanges {
		err := s.database.UpdateExchange(userID, exchangeID, exchangeData.Enabled, exchangeData.APIKey, exchangeData.SecretKey, exchangeData.Testnet, exchangeData.HyperliquidWalletAddr, exchangeData.AsterUser, exchangeData.AsterSigner, exchangeData.AsterPrivateKey)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("更新交易所 %s 失败: %v", exchangeID, err)})
			return
		}
	}

	// 重新加载该用户的所有交易员，使新配置立即生效
	err := s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 重新加载用户交易员到内存失败: %v", err)
		// 这里不返回错误，因为交易所配置已经成功更新到数据库
	}

	log.Printf("✓ 交易所配置已更新: %+v", req.Exchanges)
	c.JSON(http.StatusOK, gin.H{"message": "交易所配置已更新"})
}

// handleGetUserSignalSource 获取用户信号源配置
func (s *Server) handleGetUserSignalSource(c *gin.Context) {
	userID := c.GetString("user_id")
	source, err := s.database.GetUserSignalSource(userID)
	if err != nil {
		// 如果配置不存在，返回空配置而不是404错误
		c.JSON(http.StatusOK, gin.H{
			"coin_pool_url": "",
			"oi_top_url":    "",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"coin_pool_url": source.CoinPoolURL,
		"oi_top_url":    source.OITopURL,
	})
}

// handleSaveUserSignalSource 保存用户信号源配置
func (s *Server) handleSaveUserSignalSource(c *gin.Context) {
	userID := c.GetString("user_id")
	var req struct {
		CoinPoolURL string `json:"coin_pool_url"`
		OITopURL    string `json:"oi_top_url"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := s.database.CreateUserSignalSource(userID, req.CoinPoolURL, req.OITopURL)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("保存用户信号源配置失败: %v", err)})
		return
	}

	log.Printf("✓ 用户信号源配置已保存: user=%s, coin_pool=%s, oi_top=%s", userID, req.CoinPoolURL, req.OITopURL)
	c.JSON(http.StatusOK, gin.H{"message": "用户信号源配置已保存"})
}

// handleTraderList trader列表
func (s *Server) handleTraderList(c *gin.Context) {
	userID := c.GetString("user_id")
	traders, err := s.database.GetTraders(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("获取交易员列表失败: %v", err)})
		return
	}

	result := make([]map[string]interface{}, 0, len(traders))
	for _, trader := range traders {
		// 获取实时运行状态
		isRunning := trader.IsRunning
		if at, err := s.traderManager.GetTrader(trader.ID); err == nil {
			status := at.GetStatus()
			if running, ok := status["is_running"].(bool); ok {
				isRunning = running
			}
		}

		// 返回完整的 AIModelID（如 "admin_deepseek"），不要截断
		// 前端需要完整 ID 来验证模型是否存在（与 handleGetTraderConfig 保持一致）
		result = append(result, map[string]interface{}{
			"trader_id":       trader.ID,
			"trader_name":     trader.Name,
			"ai_model":        trader.AIModelID, // 使用完整 ID
			"exchange_id":     trader.ExchangeID,
			"is_running":      isRunning,
			"initial_balance": trader.InitialBalance,
		})
	}

	c.JSON(http.StatusOK, result)
}

// handleGetTraderConfig 获取交易员详细配置
func (s *Server) handleGetTraderConfig(c *gin.Context) {
	userID := c.GetString("user_id")
	traderID := c.Param("id")

	if traderID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "交易员ID不能为空"})
		return
	}

	traderConfig, _, _, err := s.database.GetTraderConfig(userID, traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("获取交易员配置失败: %v", err)})
		return
	}

	// 获取实时运行状态
	isRunning := traderConfig.IsRunning
	if at, err := s.traderManager.GetTrader(traderID); err == nil {
		status := at.GetStatus()
		if running, ok := status["is_running"].(bool); ok {
			isRunning = running
		}
	}

	// 返回完整的模型ID，不做转換，保持与前端模型列表一致
	result := map[string]interface{}{
		"trader_id":              traderConfig.ID,
		"trader_name":            traderConfig.Name,
		"ai_model":               traderConfig.AIModelID, // 使用完整 ID
		"exchange_id":            traderConfig.ExchangeID,
		"initial_balance":        traderConfig.InitialBalance,
		"scan_interval_minutes":  traderConfig.ScanIntervalMinutes,
		"btc_eth_leverage":       traderConfig.BTCETHLeverage,
		"altcoin_leverage":       traderConfig.AltcoinLeverage,
		"trading_symbols":        traderConfig.TradingSymbols,
		"custom_prompt":          traderConfig.CustomPrompt,
		"override_base_prompt":   traderConfig.OverrideBasePrompt,
		"system_prompt_template": traderConfig.SystemPromptTemplate,
		"is_cross_margin":        traderConfig.IsCrossMargin,
		"use_coin_pool":          traderConfig.UseCoinPool,
		"use_oi_top":             traderConfig.UseOITop,
		"is_running":             isRunning,
	}

	c.JSON(http.StatusOK, result)
}

// handleStatus 系统状态
func (s *Server) handleStatus(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	status := trader.GetStatus()
	c.JSON(http.StatusOK, status)
}

// handleAccount 账户信息
func (s *Server) handleAccount(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	log.Printf("📊 收到账户信息请求 [%s]", trader.GetName())
	account, err := trader.GetAccountInfo()
	if err != nil {
		log.Printf("❌ 获取账户信息失败 [%s]: %v", trader.GetName(), err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取账户信息失败: %v", err),
		})
		return
	}

	log.Printf("✓ 返回账户信息 [%s]: 净值=%.2f, 可用=%.2f, 盈亏=%.2f (%.2f%%)",
		trader.GetName(),
		account["total_equity"],
		account["available_balance"],
		account["total_pnl"],
		account["total_pnl_pct"])
	c.JSON(http.StatusOK, account)
}

// handlePositions 持仓列表
func (s *Server) handlePositions(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	positions, err := trader.GetPositions()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取持仓列表失败: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, positions)
}

// handleDecisions 决策日志列表
func (s *Server) handleDecisions(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// 获取所有历史决策记录（无限制）
	records, err := trader.GetDecisionLogger().GetLatestRecords(10000)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取决策日志失败: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, records)
}

// handleLatestDecisions 最新决策日志（最近5条，最新的在前）
func (s *Server) handleLatestDecisions(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	records, err := trader.GetDecisionLogger().GetLatestRecords(5)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取决策日志失败: %v", err),
		})
		return
	}

	// 反转数组，让最新的在前面（用于列表显示）
	// GetLatestRecords返回的是从旧到新（用于图表），这里需要从新到旧
	for i, j := 0, len(records)-1; i < j; i, j = i+1, j-1 {
		records[i], records[j] = records[j], records[i]
	}

	c.JSON(http.StatusOK, records)
}

// handleStatistics 统计信息
func (s *Server) handleStatistics(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	stats, err := trader.GetDecisionLogger().GetStatistics()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取统计信息失败: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// handleCompetition 竞赛总览（对比所有trader）
func (s *Server) handleCompetition(c *gin.Context) {
	userID := c.GetString("user_id")

	// 确保用户的交易员已加载到内存中
	err := s.traderManager.LoadUserTraders(s.database, userID)
	if err != nil {
		log.Printf("⚠️ 加载用户 %s 的交易员失败: %v", userID, err)
	}

	competition, err := s.traderManager.GetCompetitionData()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取竞赛数据失败: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, competition)
}

// handleEquityHistory 收益率历史数据
func (s *Server) handleEquityHistory(c *gin.Context) {
	userID := c.GetString("user_id")
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// 获取尽可能多的历史数据（几天的数据）
	// 每3分钟一个周期：10000条 = 约20天的数据
	records, err := trader.GetDecisionLogger().GetLatestRecords(10000)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("获取历史数据失败: %v", err),
		})
		return
	}

	// 构建收益率历史数据点
	type EquityPoint struct {
		Timestamp        string  `json:"timestamp"`
		TotalEquity      float64 `json:"total_equity"`      // 账户净值（wallet + unrealized）
		AvailableBalance float64 `json:"available_balance"` // 可用余额
		TotalPnL         float64 `json:"total_pnl"`         // 总盈亏（相对初始余额）
		TotalPnLPct      float64 `json:"total_pnl_pct"`     // 总盈亏百分比
		PositionCount    int     `json:"position_count"`    // 持仓数量
		MarginUsedPct    float64 `json:"margin_used_pct"`   // 保证金使用率
		CycleNumber      int     `json:"cycle_number"`
	}

	traderRecord, _, _, err := s.database.GetTraderConfig(userID, traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "无法获取交易员配置"})
		return
	}

	initialBalance := traderRecord.InitialBalance

	// 如果还是无法获取，返回错误
	if initialBalance == 0 {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "无法获取初始余额",
		})
		return
	}

	var history []EquityPoint
	for _, record := range records {
		totalEquity := record.AccountState.TotalBalance
		totalPnL := totalEquity - initialBalance
		totalPnLPct := (totalPnL / initialBalance) * 100

		history = append(history, EquityPoint{
			Timestamp:        record.Timestamp.Format("2006-01-02 15:04:05"),
			TotalEquity:      totalEquity,
			AvailableBalance: record.AccountState.AvailableBalance,
			TotalPnL:         totalPnL,
			TotalPnLPct:      totalPnLPct,
			PositionCount:    record.AccountState.PositionCount,
			MarginUsedPct:    record.AccountState.MarginUsedPct,
			CycleNumber:      record.CycleNumber,
		})
	}

	c.JSON(http.StatusOK, history)
}

// handlePerformance AI历史表现分析（用于展示AI学习和反思）
func (s *Server) handlePerformance(c *gin.Context) {
	_, traderID, err := s.getTraderFromQuery(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	trader, err := s.traderManager.GetTrader(traderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// 分析最近100个周期的交易表现（避免长期持仓的交易记录丢失）
	// 假设每3分钟一个周期，100个周期 = 5小时，足够覆盖大部分交易
	performance, err := trader.GetDecisionLogger().AnalyzePerformance(100)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("分析历史表现失败: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, performance)
}

// authMiddleware JWT认证中间件
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 如果是管理员模式，直接使用admin用户
		if auth.IsAdminMode() {
			c.Set("user_id", "admin")
			c.Set("email", "admin@localhost")
			c.Next()
			return
		}

		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "缺少Authorization头"})
			c.Abort()
			return
		}

		// 检查Bearer token格式
		tokenParts := strings.Split(authHeader, " ")
		if len(tokenParts) != 2 || tokenParts[0] != "Bearer" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "无效的Authorization格式"})
			c.Abort()
			return
		}

		// 验证JWT token
		claims, err := auth.ValidateJWT(tokenParts[1])
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "无效的token: " + err.Error()})
			c.Abort()
			return
		}

		// 将用户信息存储到上下文中
		c.Set("user_id", claims.UserID)
		c.Set("email", claims.Email)
		c.Next()
	}
}

// handleRegister 处理用户注册请求
func (s *Server) handleRegister(c *gin.Context) {
	var req struct {
		Email    string `json:"email" binding:"required,email"`
		Password string `json:"password" binding:"required,min=6"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 检查邮箱是否已存在
	_, err := s.database.GetUserByEmail(req.Email)
	if err == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "邮箱已被注册"})
		return
	}

	// 生成密码哈希
	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "密码处理失败"})
		return
	}

	// 生成OTP密钥
	otpSecret, err := auth.GenerateOTPSecret()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "OTP密钥生成失败"})
		return
	}

	// 创建用户（未验证OTP状态）
	userID := uuid.New().String()
	user := &config.User{
		ID:           userID,
		Email:        req.Email,
		PasswordHash: passwordHash,
		OTPSecret:    otpSecret,
		OTPVerified:  false,
	}

	err = s.database.CreateUser(user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建用户失败: " + err.Error()})
		return
	}

	// 返回OTP设置信息
	qrCodeURL := auth.GetOTPQRCodeURL(otpSecret, req.Email)
	c.JSON(http.StatusOK, gin.H{
		"user_id":     userID,
		"email":       req.Email,
		"otp_secret":  otpSecret,
		"qr_code_url": qrCodeURL,
		"message":     "请使用Google Authenticator扫描二维码并验证OTP",
	})
}

// handleCompleteRegistration 完成注册（验证OTP）
func (s *Server) handleCompleteRegistration(c *gin.Context) {
	var req struct {
		UserID  string `json:"user_id" binding:"required"`
		OTPCode string `json:"otp_code" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 获取用户信息
	user, err := s.database.GetUserByID(req.UserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "用户不存在"})
		return
	}

	// 验证OTP
	if !auth.VerifyOTP(user.OTPSecret, req.OTPCode) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "OTP验证码错误"})
		return
	}

	// 更新用户OTP验证状态
	err = s.database.UpdateUserOTPVerified(req.UserID, true)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新用户状态失败"})
		return
	}

	// 生成JWT token
	token, err := auth.GenerateJWT(user.ID, user.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成token失败"})
		return
	}

	// 初始化用户的默认模型和交易所配置
	err = s.initUserDefaultConfigs(user.ID)
	if err != nil {
		log.Printf("初始化用户默认配置失败: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"token":   token,
		"user_id": user.ID,
		"email":   user.Email,
		"message": "注册完成",
	})
}

// handleLogin 处理用户登录请求
func (s *Server) handleLogin(c *gin.Context) {
	var req struct {
		Email    string `json:"email" binding:"required,email"`
		Password string `json:"password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 获取用户信息
	user, err := s.database.GetUserByEmail(req.Email)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "邮箱或密码错误"})
		return
	}

	// 验证密码
	if !auth.CheckPassword(req.Password, user.PasswordHash) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "邮箱或密码错误"})
		return
	}

	// 检查OTP是否已验证
	if !user.OTPVerified {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error":              "账户未完成OTP设置",
			"user_id":            user.ID,
			"requires_otp_setup": true,
		})
		return
	}

	// 返回需要OTP验证的状态
	c.JSON(http.StatusOK, gin.H{
		"user_id":      user.ID,
		"email":        user.Email,
		"message":      "请输入Google Authenticator验证码",
		"requires_otp": true,
	})
}

// handleVerifyOTP 验证OTP并完成登录
func (s *Server) handleVerifyOTP(c *gin.Context) {
	var req struct {
		UserID  string `json:"user_id" binding:"required"`
		OTPCode string `json:"otp_code" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 获取用户信息
	user, err := s.database.GetUserByID(req.UserID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "用户不存在"})
		return
	}

	// 验证OTP
	if !auth.VerifyOTP(user.OTPSecret, req.OTPCode) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "验证码错误"})
		return
	}

	// 生成JWT token
	token, err := auth.GenerateJWT(user.ID, user.Email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "生成token失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token":   token,
		"user_id": user.ID,
		"email":   user.Email,
		"message": "登录成功",
	})
}

// initUserDefaultConfigs 为新用户初始化默认的模型和交易所配置
func (s *Server) initUserDefaultConfigs(userID string) error {
	// 注释掉自动创建默认配置，让用户手动添加
	// 这样新用户注册后不会自动有配置项
	log.Printf("用户 %s 注册完成，等待手动配置AI模型和交易所", userID)
	return nil
}

// handleGetSupportedModels 获取系统支持的AI模型列表
func (s *Server) handleGetSupportedModels(c *gin.Context) {
	// 返回系统支持的AI模型（从default用户获取）
	models, err := s.database.GetAIModels("default")
	if err != nil {
		log.Printf("❌ 获取支持的AI模型失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取支持的AI模型失败"})
		return
	}

	c.JSON(http.StatusOK, models)
}

// handleGetSupportedExchanges 获取系统支持的交易所列表
func (s *Server) handleGetSupportedExchanges(c *gin.Context) {
	// 返回系统支持的交易所（从default用户获取）
	exchanges, err := s.database.GetExchanges("default")
	if err != nil {
		log.Printf("❌ 获取支持的交易所失败: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取支持的交易所失败"})
		return
	}

	c.JSON(http.StatusOK, exchanges)
}

// Start 启动服务器
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	log.Printf("🌐 API服务器启动在 http://localhost%s", addr)
	log.Printf("📊 API文档:")
	log.Printf("  • GET  /api/health           - 健康检查")
	log.Printf("  • GET  /api/traders          - AI交易员列表")
	log.Printf("  • POST /api/traders          - 创建新的AI交易员")
	log.Printf("  • DELETE /api/traders/:id    - 删除AI交易员")
	log.Printf("  • POST /api/traders/:id/start - 启动AI交易员")
	log.Printf("  • POST /api/traders/:id/stop  - 停止AI交易员")
	log.Printf("  • GET  /api/models           - 获取AI模型配置")
	log.Printf("  • PUT  /api/models           - 更新AI模型配置")
	log.Printf("  • GET  /api/exchanges        - 获取交易所配置")
	log.Printf("  • PUT  /api/exchanges        - 更新交易所配置")
	log.Printf("  • GET  /api/status?trader_id=xxx     - 指定trader的系统状态")
	log.Printf("  • GET  /api/account?trader_id=xxx    - 指定trader的账户信息")
	log.Printf("  • GET  /api/positions?trader_id=xxx  - 指定trader的持仓列表")
	log.Printf("  • GET  /api/decisions?trader_id=xxx  - 指定trader的决策日志")
	log.Printf("  • GET  /api/decisions/latest?trader_id=xxx - 指定trader的最新决策")
	log.Printf("  • GET  /api/statistics?trader_id=xxx - 指定trader的统计信息")
	log.Printf("  • GET  /api/equity-history?trader_id=xxx - 指定trader的收益率历史数据")
	log.Printf("  • GET  /api/performance?trader_id=xxx - 指定trader的AI学习表现分析")
	log.Println()

	return s.router.Run(addr)
}

// handleGetPromptTemplates 获取所有系统提示词模板列表
func (s *Server) handleGetPromptTemplates(c *gin.Context) {
	// 导入 decision 包
	templates := decision.GetAllPromptTemplates()

	// 转换为响应格式
	response := make([]map[string]interface{}, 0, len(templates))
	for _, tmpl := range templates {
		response = append(response, map[string]interface{}{
			"name": tmpl.Name,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"templates": response,
	})
}

// handleGetPromptTemplate 获取指定名称的提示词模板内容
func (s *Server) handleGetPromptTemplate(c *gin.Context) {
	templateName := c.Param("name")

	template, err := decision.GetPromptTemplate(templateName)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("模板不存在: %s", templateName)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"name":    template.Name,
		"content": template.Content,
	})
}
