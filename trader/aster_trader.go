package trader

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// AsterTrader Aster交易平台实现
type AsterTrader struct {
	ctx        context.Context
	user       string            // 主钱包地址 (ERC20)
	signer     string            // API钱包地址
	privateKey *ecdsa.PrivateKey // API钱包私钥
	client     *http.Client
	baseURL    string

	// 缓存交易对精度信息
	symbolPrecision map[string]SymbolPrecision
	mu              sync.RWMutex
}

// SymbolPrecision 交易对精度信息
type SymbolPrecision struct {
	PricePrecision    int
	QuantityPrecision int
	TickSize          float64 // 价格步进值
	StepSize          float64 // 数量步进值
}

// NewAsterTrader 创建Aster交易器
// user: 主钱包地址 (登录地址)
// signer: API钱包地址 (从 https://www.asterdex.com/en/api-wallet 获取)
// privateKey: API钱包私钥 (从 https://www.asterdex.com/en/api-wallet 获取)
func NewAsterTrader(user, signer, privateKeyHex string) (*AsterTrader, error) {
	// 解析私钥
	privKey, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("解析私钥失败: %w", err)
	}

	return &AsterTrader{
		ctx:             context.Background(),
		user:            user,
		signer:          signer,
		privateKey:      privKey,
		symbolPrecision: make(map[string]SymbolPrecision),
		client: &http.Client{
			Timeout: 30 * time.Second, // 增加到30秒
			Transport: &http.Transport{
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		baseURL: "https://fapi.asterdex.com",
	}, nil
}

// genNonce 生成微秒时间戳
func (t *AsterTrader) genNonce() uint64 {
	return uint64(time.Now().UnixMicro())
}

// getPrecision 获取交易对精度信息
func (t *AsterTrader) getPrecision(symbol string) (SymbolPrecision, error) {
	t.mu.RLock()
	if prec, ok := t.symbolPrecision[symbol]; ok {
		t.mu.RUnlock()
		return prec, nil
	}
	t.mu.RUnlock()

	// 获取交易所信息
	resp, err := t.client.Get(t.baseURL + "/fapi/v3/exchangeInfo")
	if err != nil {
		return SymbolPrecision{}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var info struct {
		Symbols []struct {
			Symbol            string                   `json:"symbol"`
			PricePrecision    int                      `json:"pricePrecision"`
			QuantityPrecision int                      `json:"quantityPrecision"`
			Filters           []map[string]interface{} `json:"filters"`
		} `json:"symbols"`
	}

	if err := json.Unmarshal(body, &info); err != nil {
		return SymbolPrecision{}, err
	}

	// 缓存所有交易对的精度
	t.mu.Lock()
	for _, s := range info.Symbols {
		prec := SymbolPrecision{
			PricePrecision:    s.PricePrecision,
			QuantityPrecision: s.QuantityPrecision,
		}

		// 解析filters获取tickSize和stepSize
		for _, filter := range s.Filters {
			filterType, _ := filter["filterType"].(string)
			switch filterType {
			case "PRICE_FILTER":
				if tickSizeStr, ok := filter["tickSize"].(string); ok {
					prec.TickSize, _ = strconv.ParseFloat(tickSizeStr, 64)
				}
			case "LOT_SIZE":
				if stepSizeStr, ok := filter["stepSize"].(string); ok {
					prec.StepSize, _ = strconv.ParseFloat(stepSizeStr, 64)
				}
			}
		}

		t.symbolPrecision[s.Symbol] = prec
	}
	t.mu.Unlock()

	if prec, ok := t.symbolPrecision[symbol]; ok {
		return prec, nil
	}

	return SymbolPrecision{}, fmt.Errorf("未找到交易对 %s 的精度信息", symbol)
}

// roundToTickSize 将价格/数量四舍五入到tick size/step size的整数倍
func roundToTickSize(value float64, tickSize float64) float64 {
	if tickSize <= 0 {
		return value
	}
	// 计算有多少个tick size
	steps := value / tickSize
	// 四舍五入到最近的整数
	roundedSteps := math.Round(steps)
	// 乘回tick size
	return roundedSteps * tickSize
}

// formatPrice 格式化价格到正确精度和tick size
func (t *AsterTrader) formatPrice(symbol string, price float64) (float64, error) {
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return 0, err
	}

	// 优先使用tick size，确保价格是tick size的整数倍
	if prec.TickSize > 0 {
		return roundToTickSize(price, prec.TickSize), nil
	}

	// 如果没有tick size，则按精度四舍五入
	multiplier := math.Pow10(prec.PricePrecision)
	return math.Round(price*multiplier) / multiplier, nil
}

// formatQuantity 格式化数量到正确精度和step size
func (t *AsterTrader) formatQuantity(symbol string, quantity float64) (float64, error) {
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return 0, err
	}

	// 优先使用step size，确保数量是step size的整数倍
	if prec.StepSize > 0 {
		return roundToTickSize(quantity, prec.StepSize), nil
	}

	// 如果没有step size，则按精度四舍五入
	multiplier := math.Pow10(prec.QuantityPrecision)
	return math.Round(quantity*multiplier) / multiplier, nil
}

// formatFloatWithPrecision 将浮点数格式化为指定精度的字符串（去除末尾的0）
func (t *AsterTrader) formatFloatWithPrecision(value float64, precision int) string {
	// 使用指定精度格式化
	formatted := strconv.FormatFloat(value, 'f', precision, 64)

	// 去除末尾的0和小数点（如果有）
	formatted = strings.TrimRight(formatted, "0")
	formatted = strings.TrimRight(formatted, ".")

	return formatted
}

// normalizeAndStringify 对参数进行规范化并序列化为JSON字符串（按key排序）
func (t *AsterTrader) normalizeAndStringify(params map[string]interface{}) (string, error) {
	normalized, err := t.normalize(params)
	if err != nil {
		return "", err
	}
	bs, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// normalize 递归规范化参数（按key排序，所有值转为字符串）
func (t *AsterTrader) normalize(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		newMap := make(map[string]interface{}, len(keys))
		for _, k := range keys {
			nv, err := t.normalize(val[k])
			if err != nil {
				return nil, err
			}
			newMap[k] = nv
		}
		return newMap, nil
	case []interface{}:
		out := make([]interface{}, 0, len(val))
		for _, it := range val {
			nv, err := t.normalize(it)
			if err != nil {
				return nil, err
			}
			out = append(out, nv)
		}
		return out, nil
	case string:
		return val, nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return fmt.Sprintf("%d", val), nil
	case float64:
		return fmt.Sprintf("%v", val), nil
	case bool:
		return fmt.Sprintf("%v", val), nil
	default:
		// 其他类型转为字符串
		return fmt.Sprintf("%v", val), nil
	}
}

// sign 对请求参数进行签名
func (t *AsterTrader) sign(params map[string]interface{}, nonce uint64) error {
	// 添加时间戳和接收窗口
	params["recvWindow"] = "50000"
	params["timestamp"] = strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)

	// 规范化参数为JSON字符串
	jsonStr, err := t.normalizeAndStringify(params)
	if err != nil {
		return err
	}

	// ABI编码: (string, address, address, uint256)
	addrUser := common.HexToAddress(t.user)
	addrSigner := common.HexToAddress(t.signer)
	nonceBig := new(big.Int).SetUint64(nonce)

	tString, _ := abi.NewType("string", "", nil)
	tAddress, _ := abi.NewType("address", "", nil)
	tUint256, _ := abi.NewType("uint256", "", nil)

	arguments := abi.Arguments{
		{Type: tString},
		{Type: tAddress},
		{Type: tAddress},
		{Type: tUint256},
	}

	packed, err := arguments.Pack(jsonStr, addrUser, addrSigner, nonceBig)
	if err != nil {
		return fmt.Errorf("ABI编码失败: %w", err)
	}

	// Keccak256哈希
	hash := crypto.Keccak256(packed)

	// 以太坊签名消息前缀
	prefixedMsg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(hash), hash)
	msgHash := crypto.Keccak256Hash([]byte(prefixedMsg))

	// ECDSA签名
	sig, err := crypto.Sign(msgHash.Bytes(), t.privateKey)
	if err != nil {
		return fmt.Errorf("签名失败: %w", err)
	}

	// 将v从0/1转换为27/28
	if len(sig) != 65 {
		return fmt.Errorf("签名长度异常: %d", len(sig))
	}
	sig[64] += 27

	// 添加签名参数
	params["user"] = t.user
	params["signer"] = t.signer
	params["signature"] = "0x" + hex.EncodeToString(sig)
	params["nonce"] = nonce

	return nil
}

// request 发送HTTP请求（带重试机制）
func (t *AsterTrader) request(method, endpoint string, params map[string]interface{}) ([]byte, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// 每次重试都生成新的nonce和签名
		nonce := t.genNonce()
		paramsCopy := make(map[string]interface{})
		for k, v := range params {
			paramsCopy[k] = v
		}

		// 签名
		if err := t.sign(paramsCopy, nonce); err != nil {
			return nil, err
		}

		body, err := t.doRequest(method, endpoint, paramsCopy)
		if err == nil {
			return body, nil
		}

		lastErr = err

		// 如果是网络超时或临时错误，重试
		if strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "connection reset") ||
			strings.Contains(err.Error(), "EOF") {
			if attempt < maxRetries {
				waitTime := time.Duration(attempt) * time.Second
				time.Sleep(waitTime)
				continue
			}
		}

		// 其他错误（如400/401等）不重试
		return nil, err
	}

	return nil, fmt.Errorf("请求失败（已重试%d次）: %w", maxRetries, lastErr)
}

// doRequest 执行实际的HTTP请求
func (t *AsterTrader) doRequest(method, endpoint string, params map[string]interface{}) ([]byte, error) {
	fullURL := t.baseURL + endpoint
	method = strings.ToUpper(method)

	switch method {
	case "POST":
		// POST请求：参数放在表单body中
		form := url.Values{}
		for k, v := range params {
			form.Set(k, fmt.Sprintf("%v", v))
		}
		req, err := http.NewRequest("POST", fullURL, strings.NewReader(form.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := t.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
		return body, nil

	case "GET", "DELETE":
		// GET/DELETE请求：参数放在querystring中
		q := url.Values{}
		for k, v := range params {
			q.Set(k, fmt.Sprintf("%v", v))
		}
		u, _ := url.Parse(fullURL)
		u.RawQuery = q.Encode()

		req, err := http.NewRequest(method, u.String(), nil)
		if err != nil {
			return nil, err
		}

		resp, err := t.client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
		return body, nil

	default:
		return nil, fmt.Errorf("不支持的HTTP方法: %s", method)
	}
}

// GetBalance 获取账户余额
func (t *AsterTrader) GetBalance() (map[string]interface{}, error) {
	params := make(map[string]interface{})
	body, err := t.request("GET", "/fapi/v3/balance", params)
	if err != nil {
		return nil, err
	}

	var balances []map[string]interface{}
	if err := json.Unmarshal(body, &balances); err != nil {
		return nil, err
	}

	// 查找USDT余额
	totalBalance := 0.0
	availableBalance := 0.0
	crossUnPnl := 0.0

	for _, bal := range balances {
		if asset, ok := bal["asset"].(string); ok && asset == "USDT" {
			if wb, ok := bal["balance"].(string); ok {
				totalBalance, _ = strconv.ParseFloat(wb, 64)
			}
			if avail, ok := bal["availableBalance"].(string); ok {
				availableBalance, _ = strconv.ParseFloat(avail, 64)
			}
			if unpnl, ok := bal["crossUnPnl"].(string); ok {
				crossUnPnl, _ = strconv.ParseFloat(unpnl, 64)
			}
			break
		}
	}

	// 返回与Binance相同的字段名，确保AutoTrader能正确解析
	return map[string]interface{}{
		"totalWalletBalance":    totalBalance,
		"availableBalance":      availableBalance,
		"totalUnrealizedProfit": crossUnPnl,
	}, nil
}

// GetPositions 获取持仓信息
func (t *AsterTrader) GetPositions() ([]map[string]interface{}, error) {
	params := make(map[string]interface{})
	body, err := t.request("GET", "/fapi/v3/positionRisk", params)
	if err != nil {
		return nil, err
	}

	var positions []map[string]interface{}
	if err := json.Unmarshal(body, &positions); err != nil {
		return nil, err
	}

	result := []map[string]interface{}{}
	for _, pos := range positions {
		posAmtStr, ok := pos["positionAmt"].(string)
		if !ok {
			continue
		}

		posAmt, _ := strconv.ParseFloat(posAmtStr, 64)
		if posAmt == 0 {
			continue // 跳过空仓位
		}

		entryPrice, _ := strconv.ParseFloat(pos["entryPrice"].(string), 64)
		markPrice, _ := strconv.ParseFloat(pos["markPrice"].(string), 64)
		unRealizedProfit, _ := strconv.ParseFloat(pos["unRealizedProfit"].(string), 64)
		leverageVal, _ := strconv.ParseFloat(pos["leverage"].(string), 64)
		liquidationPrice, _ := strconv.ParseFloat(pos["liquidationPrice"].(string), 64)

		// 判断方向（与Binance一致）
		side := "long"
		if posAmt < 0 {
			side = "short"
			posAmt = -posAmt
		}

		// 返回与Binance相同的字段名
		result = append(result, map[string]interface{}{
			"symbol":           pos["symbol"],
			"side":             side,
			"positionAmt":      posAmt,
			"entryPrice":       entryPrice,
			"markPrice":        markPrice,
			"unRealizedProfit": unRealizedProfit,
			"leverage":         leverageVal,
			"liquidationPrice": liquidationPrice,
		})
	}

	return result, nil
}

// OpenLong 开多单
func (t *AsterTrader) OpenLong(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	// 开仓前先取消所有挂单,防止残留挂单导致仓位叠加
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败(继续开仓): %v", err)
	}

	// 先设置杠杆
	if err := t.SetLeverage(symbol, leverage); err != nil {
		return nil, fmt.Errorf("设置杠杆失败: %w", err)
	}

	// 获取当前价格
	price, err := t.GetMarketPrice(symbol)
	if err != nil {
		return nil, err
	}

	// 使用限价单模拟市价单（价格设置得稍高一些以确保成交）
	limitPrice := price * 1.01

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, limitPrice)
	if err != nil {
		return nil, err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return nil, err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	log.Printf("  📏 精度处理: 价格 %.8f -> %s (精度=%d), 数量 %.8f -> %s (精度=%d)",
		limitPrice, priceStr, prec.PricePrecision, quantity, qtyStr, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "LIMIT",
		"side":         "BUY",
		"timeInForce":  "GTC",
		"quantity":     qtyStr,
		"price":        priceStr,
	}

	body, err := t.request("POST", "/fapi/v3/order", params)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// OpenShort 开空单
func (t *AsterTrader) OpenShort(symbol string, quantity float64, leverage int) (map[string]interface{}, error) {
	// 开仓前先取消所有挂单,防止残留挂单导致仓位叠加
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败(继续开仓): %v", err)
	}

	// 先设置杠杆
	if err := t.SetLeverage(symbol, leverage); err != nil {
		return nil, fmt.Errorf("设置杠杆失败: %w", err)
	}

	// 获取当前价格
	price, err := t.GetMarketPrice(symbol)
	if err != nil {
		return nil, err
	}

	// 使用限价单模拟市价单（价格设置得稍低一些以确保成交）
	limitPrice := price * 0.99

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, limitPrice)
	if err != nil {
		return nil, err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return nil, err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	log.Printf("  📏 精度处理: 价格 %.8f -> %s (精度=%d), 数量 %.8f -> %s (精度=%d)",
		limitPrice, priceStr, prec.PricePrecision, quantity, qtyStr, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "LIMIT",
		"side":         "SELL",
		"timeInForce":  "GTC",
		"quantity":     qtyStr,
		"price":        priceStr,
	}

	body, err := t.request("POST", "/fapi/v3/order", params)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// CloseLong 平多单
func (t *AsterTrader) CloseLong(symbol string, quantity float64) (map[string]interface{}, error) {
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
		log.Printf("  📊 获取到多仓数量: %.8f", quantity)
	}

	price, err := t.GetMarketPrice(symbol)
	if err != nil {
		return nil, err
	}

	limitPrice := price * 0.99

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, limitPrice)
	if err != nil {
		return nil, err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return nil, err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	log.Printf("  📏 精度处理: 价格 %.8f -> %s (精度=%d), 数量 %.8f -> %s (精度=%d)",
		limitPrice, priceStr, prec.PricePrecision, quantity, qtyStr, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "LIMIT",
		"side":         "SELL",
		"timeInForce":  "GTC",
		"quantity":     qtyStr,
		"price":        priceStr,
	}

	body, err := t.request("POST", "/fapi/v3/order", params)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	log.Printf("✓ 平多仓成功: %s 数量: %s", symbol, qtyStr)

	// 平仓后取消该币种的所有挂单(止损止盈单)
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", err)
	}

	return result, nil
}

// CloseShort 平空单
func (t *AsterTrader) CloseShort(symbol string, quantity float64) (map[string]interface{}, error) {
	// 如果数量为0，获取当前持仓数量
	if quantity == 0 {
		positions, err := t.GetPositions()
		if err != nil {
			return nil, err
		}

		for _, pos := range positions {
			if pos["symbol"] == symbol && pos["side"] == "short" {
				// Aster的GetPositions已经将空仓数量转换为正数，直接使用
				quantity = pos["positionAmt"].(float64)
				break
			}
		}

		if quantity == 0 {
			return nil, fmt.Errorf("没有找到 %s 的空仓", symbol)
		}
		log.Printf("  📊 获取到空仓数量: %.8f", quantity)
	}

	price, err := t.GetMarketPrice(symbol)
	if err != nil {
		return nil, err
	}

	limitPrice := price * 1.01

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, limitPrice)
	if err != nil {
		return nil, err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return nil, err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return nil, err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	log.Printf("  📏 精度处理: 价格 %.8f -> %s (精度=%d), 数量 %.8f -> %s (精度=%d)",
		limitPrice, priceStr, prec.PricePrecision, quantity, qtyStr, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "LIMIT",
		"side":         "BUY",
		"timeInForce":  "GTC",
		"quantity":     qtyStr,
		"price":        priceStr,
	}

	body, err := t.request("POST", "/fapi/v3/order", params)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	log.Printf("✓ 平空仓成功: %s 数量: %s", symbol, qtyStr)

	// 平仓后取消该币种的所有挂单(止损止盈单)
	if err := t.CancelAllOrders(symbol); err != nil {
		log.Printf("  ⚠ 取消挂单失败: %v", err)
	}

	return result, nil
}

// SetMarginMode 设置仓位模式
func (t *AsterTrader) SetMarginMode(symbol string, isCrossMargin bool) error {
	// Aster支持仓位模式设置
	// API格式与币安相似：CROSSED(全仓) / ISOLATED(逐仓)
	marginType := "CROSSED"
	if !isCrossMargin {
		marginType = "ISOLATED"
	}

	params := map[string]interface{}{
		"symbol":     symbol,
		"marginType": marginType,
	}

	// 使用request方法调用API
	_, err := t.request("POST", "/fapi/v3/marginType", params)
	if err != nil {
		// 如果错误表示无需更改，忽略错误
		if strings.Contains(err.Error(), "No need to change") ||
			strings.Contains(err.Error(), "Margin type cannot be changed") {
			log.Printf("  ✓ %s 仓位模式已是 %s 或有持仓无法更改", symbol, marginType)
			return nil
		}
		// 检测多资产模式（错误码 -4168）
		if strings.Contains(err.Error(), "Multi-Assets mode") ||
			strings.Contains(err.Error(), "-4168") ||
			strings.Contains(err.Error(), "4168") {
			log.Printf("  ⚠️ %s 检测到多资产模式，强制使用全仓模式", symbol)
			log.Printf("  💡 提示：如需使用逐仓模式，请在交易所关闭多资产模式")
			return nil
		}
		// 检测统一账户 API
		if strings.Contains(err.Error(), "unified") ||
			strings.Contains(err.Error(), "portfolio") ||
			strings.Contains(err.Error(), "Portfolio") {
			log.Printf("  ❌ %s 检测到统一账户 API，无法进行合约交易", symbol)
			return fmt.Errorf("请使用「现货与合约交易」API 权限，不要使用「统一账户 API」")
		}
		log.Printf("  ⚠️ 设置仓位模式失败: %v", err)
		// 不返回错误，让交易继续
		return nil
	}

	log.Printf("  ✓ %s 仓位模式已设置为 %s", symbol, marginType)
	return nil
}

// SetLeverage 设置杠杆倍数
func (t *AsterTrader) SetLeverage(symbol string, leverage int) error {
	params := map[string]interface{}{
		"symbol":   symbol,
		"leverage": leverage,
	}

	_, err := t.request("POST", "/fapi/v3/leverage", params)
	return err
}

// GetMarketPrice 获取市场价格
func (t *AsterTrader) GetMarketPrice(symbol string) (float64, error) {
	// 使用ticker接口获取当前价格
	resp, err := t.client.Get(fmt.Sprintf("%s/fapi/v3/ticker/price?symbol=%s", t.baseURL, symbol))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	priceStr, ok := result["price"].(string)
	if !ok {
		return 0, errors.New("无法获取价格")
	}

	return strconv.ParseFloat(priceStr, 64)
}

// SetStopLoss 设置止损
func (t *AsterTrader) SetStopLoss(symbol string, positionSide string, quantity, stopPrice float64) error {
	side := "SELL"
	if positionSide == "SHORT" {
		side = "BUY"
	}

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, stopPrice)
	if err != nil {
		return err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "STOP_MARKET",
		"side":         side,
		"stopPrice":    priceStr,
		"quantity":     qtyStr,
		"timeInForce":  "GTC",
	}

	_, err = t.request("POST", "/fapi/v3/order", params)
	return err
}

// SetTakeProfit 设置止盈
func (t *AsterTrader) SetTakeProfit(symbol string, positionSide string, quantity, takeProfitPrice float64) error {
	side := "SELL"
	if positionSide == "SHORT" {
		side = "BUY"
	}

	// 格式化价格和数量到正确精度
	formattedPrice, err := t.formatPrice(symbol, takeProfitPrice)
	if err != nil {
		return err
	}
	formattedQty, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return err
	}

	// 获取精度信息
	prec, err := t.getPrecision(symbol)
	if err != nil {
		return err
	}

	// 转换为字符串，使用正确的精度格式
	priceStr := t.formatFloatWithPrecision(formattedPrice, prec.PricePrecision)
	qtyStr := t.formatFloatWithPrecision(formattedQty, prec.QuantityPrecision)

	params := map[string]interface{}{
		"symbol":       symbol,
		"positionSide": "BOTH",
		"type":         "TAKE_PROFIT_MARKET",
		"side":         side,
		"stopPrice":    priceStr,
		"quantity":     qtyStr,
		"timeInForce":  "GTC",
	}

	_, err = t.request("POST", "/fapi/v3/order", params)
	return err
}

// CancelAllOrders 取消所有订单
func (t *AsterTrader) CancelAllOrders(symbol string) error {
	params := map[string]interface{}{
		"symbol": symbol,
	}

	_, err := t.request("DELETE", "/fapi/v3/allOpenOrders", params)
	return err
}

// CancelStopOrders 取消该币种的止盈/止损单（用于调整止盈止损位置）
func (t *AsterTrader) CancelStopOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	params := map[string]interface{}{
		"symbol": symbol,
	}

	body, err := t.request("GET", "/fapi/v3/openOrders", params)
	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	var orders []map[string]interface{}
	if err := json.Unmarshal(body, &orders); err != nil {
		return fmt.Errorf("解析订单数据失败: %w", err)
	}

	// 过滤出止盈止损单并取消
	canceledCount := 0
	for _, order := range orders {
		orderType, _ := order["type"].(string)

		// 只取消止损和止盈订单
		if orderType == "STOP_MARKET" ||
			orderType == "TAKE_PROFIT_MARKET" ||
			orderType == "STOP" ||
			orderType == "TAKE_PROFIT" {

			orderID, _ := order["orderId"].(float64)
			cancelParams := map[string]interface{}{
				"symbol":  symbol,
				"orderId": int64(orderID),
			}

			_, err := t.request("DELETE", "/fapi/v3/order", cancelParams)
			if err != nil {
				log.Printf("  ⚠ 取消订单 %d 失败: %v", int64(orderID), err)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消 %s 的止盈/止损单 (订单ID: %d, 类型: %s)",
				symbol, int64(orderID), orderType)
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
func (t *AsterTrader) CancelStopLossOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	params := map[string]interface{}{
		"symbol": symbol,
	}

	body, err := t.request("GET", "/fapi/v3/openOrders", params)
	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	var orders []map[string]interface{}
	if err := json.Unmarshal(body, &orders); err != nil {
		return fmt.Errorf("解析订单数据失败: %w", err)
	}

	// 过滤出止损单并取消（取消所有方向的止损单，包括LONG和SHORT）
	canceledCount := 0
	var cancelErrors []error
	for _, order := range orders {
		orderType, _ := order["type"].(string)

		// 只取消止损订单（不取消止盈订单）
		if orderType == "STOP_MARKET" || orderType == "STOP" {
			orderID, _ := order["orderId"].(float64)
			positionSide, _ := order["positionSide"].(string)
			cancelParams := map[string]interface{}{
				"symbol":  symbol,
				"orderId": int64(orderID),
			}

			_, err := t.request("DELETE", "/fapi/v1/order", cancelParams)
			if err != nil {
				wrappedErr := fmt.Errorf("订单ID %d: %w", int64(orderID), err)
				cancelErrors = append(cancelErrors, wrappedErr)
				log.Printf("  ⚠ 取消止损单失败: %v", wrappedErr)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消止损单 (订单ID: %d, 类型: %s, 方向: %s)", int64(orderID), orderType, positionSide)
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
func (t *AsterTrader) CancelTakeProfitOrders(symbol string) error {
	// 获取该币种的所有未完成订单
	params := map[string]interface{}{
		"symbol": symbol,
	}

	body, err := t.request("GET", "/fapi/v3/openOrders", params)
	if err != nil {
		return fmt.Errorf("获取未完成订单失败: %w", err)
	}

	var orders []map[string]interface{}
	if err := json.Unmarshal(body, &orders); err != nil {
		return fmt.Errorf("解析订单数据失败: %w", err)
	}

	// 过滤出止盈单并取消（取消所有方向的止盈单，包括LONG和SHORT）
	canceledCount := 0
	var cancelErrors []error
	for _, order := range orders {
		orderType, _ := order["type"].(string)

		// 只取消止盈订单（不取消止损订单）
		if orderType == "TAKE_PROFIT_MARKET" || orderType == "TAKE_PROFIT" {
			orderID, _ := order["orderId"].(float64)
			positionSide, _ := order["positionSide"].(string)
			cancelParams := map[string]interface{}{
				"symbol":  symbol,
				"orderId": int64(orderID),
			}

			_, err := t.request("DELETE", "/fapi/v1/order", cancelParams)
			if err != nil {
				wrappedErr := fmt.Errorf("订单ID %d: %w", int64(orderID), err)
				cancelErrors = append(cancelErrors, wrappedErr)
				log.Printf("  ⚠ 取消止盈单失败: %v", wrappedErr)
				continue
			}

			canceledCount++
			log.Printf("  ✓ 已取消止盈单 (订单ID: %d, 类型: %s, 方向: %s)", int64(orderID), orderType, positionSide)
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

// FormatQuantity 格式化数量（实现Trader接口）
func (t *AsterTrader) FormatQuantity(symbol string, quantity float64) (string, error) {
	formatted, err := t.formatQuantity(symbol, quantity)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", formatted), nil
}
