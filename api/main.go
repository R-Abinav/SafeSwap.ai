package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"github.com/joho/godotenv"
)

// ===== CONFIGURATION =====
var (
	// API Keys (set these as environment variables or replace with your keys)
	COINGECKO_API_KEY = os.Getenv("COINGECKO_API_KEY") // Optional for free tier
	CMC_API_KEY       = os.Getenv("CMC_API_KEY")       // Required - get from https://coinmarketcap.com/api/

	// File paths
	CG_CSV_PATH  = "./data/cg_data_02.csv"
	CMC_CSV_PATH = "./data/cmc_data_02.csv"
	LOG_PATH     = "./data/api_scraper.log"

	// Tokens to track (CoinGecko IDs)
	TOKENS = []string{
		"bitcoin", "ethereum", "solana", "cardano", "ripple",
		"polkadot", "dogecoin", "avalanche-2", "chainlink", "polygon",
		"uniswap", "litecoin", "stellar", "cosmos", "monero",
		"tron", "ethereum-classic", "filecoin", "hedera-hashgraph", "aptos",
	}

	// Rate limiting (calls per minute)
	CG_RATE_LIMIT  = 10              // CoinGecko free tier: 10-15 calls/min
	CMC_RATE_LIMIT = 30              // CMC free tier: 30 calls/min
	CG_DELAY       = 7 * time.Second // 60s / 10 calls = 6s, using 7s to be safe
	CMC_DELAY      = 3 * time.Second // 60s / 30 calls = 2s, using 3s to be safe

	// Historical data range (CoinGecko supports up to 365 days on free tier)
	DAYS_HISTORICAL = 365

	// Run mode: set to true to only collect current snapshots (for repeated runs)
	SKIP_HISTORICAL = false // Set to true after first run
)

var TOKEN_METADATA = map[string]struct {
	Symbol string
	Name   string
}{
	"bitcoin":            {"btc", "Bitcoin"},
	"ethereum":           {"eth", "Ethereum"},
	"solana":             {"sol", "Solana"},
	"cardano":            {"ada", "Cardano"},
	"ripple":             {"xrp", "XRP"},
	"polkadot":           {"dot", "Polkadot"},
	"dogecoin":           {"doge", "Dogecoin"},
	"avalanche-2":        {"avax", "Avalanche"},
	"chainlink":          {"link", "Chainlink"},
	"polygon":            {"matic", "Polygon"},
	"uniswap":            {"uni", "Uniswap"},
	"litecoin":           {"ltc", "Litecoin"},
	"stellar":            {"xlm", "Stellar"},
	"cosmos":             {"atom", "Cosmos"},
	"monero":             {"xmr", "Monero"},
	"tron":               {"trx", "TRON"},
	"ethereum-classic":   {"etc", "Ethereum Classic"},
	"filecoin":           {"fil", "Filecoin"},
	"hedera-hashgraph":   {"hbar", "Hedera"},
	"aptos":              {"apt", "Aptos"},
}

// ===== DATA STRUCTURES =====
type CoinGeckoHistoricalResponse struct {
	Prices       [][]float64 `json:"prices"`
	MarketCaps   [][]float64 `json:"market_caps"`
	TotalVolumes [][]float64 `json:"total_volumes"`
}

type CoinGeckoCurrentResponse struct {
	ID                           string  `json:"id"`
	Symbol                       string  `json:"symbol"`
	Name                         string  `json:"name"`
	CurrentPrice                 float64 `json:"current_price"`
	MarketCap                    float64 `json:"market_cap"`
	TotalVolume                  float64 `json:"total_volume"`
	High24h                      float64 `json:"high_24h"`
	Low24h                       float64 `json:"low_24h"`
	PriceChange24h               float64 `json:"price_change_24h"`
	PriceChangePercentage24h     float64 `json:"price_change_percentage_24h"`
	CirculatingSupply            float64 `json:"circulating_supply"`
	TotalSupply                  float64 `json:"total_supply"`
	ATH                          float64 `json:"ath"`
	ATHDate                      string  `json:"ath_date"`
}

type CMCQuoteResponse struct {
	Data   map[string]CMCCoinData `json:"data"`
	Status CMCStatus              `json:"status"`
}

type CMCCoinData struct {
	ID                int                `json:"id"`
	Name              string             `json:"name"`
	Symbol            string             `json:"symbol"`
	Slug              string             `json:"slug"`
	CirculatingSupply float64            `json:"circulating_supply"`
	TotalSupply       float64            `json:"total_supply"`
	MaxSupply         float64            `json:"max_supply"`
	Quote             map[string]CMCQuote `json:"quote"`
}

type CMCQuote struct {
	Price              float64 `json:"price"`
	Volume24h          float64 `json:"volume_24h"`
	VolumeChange24h    float64 `json:"volume_change_24h"`
	PercentChange1h    float64 `json:"percent_change_1h"`
	PercentChange24h   float64 `json:"percent_change_24h"`
	PercentChange7d    float64 `json:"percent_change_7d"`
	MarketCap          float64 `json:"market_cap"`
	MarketCapDominance float64 `json:"market_cap_dominance"`
	LastUpdated        string  `json:"last_updated"`
}

type CMCStatus struct {
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

// ===== MAIN =====
func main() {
	godotenv.Load();
	startTime := time.Now()

	// Setup logging
	os.MkdirAll("./data", 0755)
	logFile, err := os.OpenFile(LOG_PATH, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	fmt.Println("╔════════════════════════════════════════════════════╗")
	fmt.Println("║   CRYPTO API DATA COLLECTOR v3.0                  ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Printf("\n📊 Collecting data for %d tokens\n", len(TOKENS))
	fmt.Printf("📁 CoinGecko output: %s\n", CG_CSV_PATH)
	fmt.Printf("📁 CoinMarketCap output: %s\n", CMC_CSV_PATH)

	// Check if files exist (determines if this is first run)
	cgExists := fileExists(CG_CSV_PATH)
	cmcExists := fileExists(CMC_CSV_PATH)

	if cgExists || cmcExists {
		fmt.Println("\n✅ Existing data files detected")
		fmt.Println("📝 Running in APPEND mode - only collecting current snapshots")
		SKIP_HISTORICAL = true
	} else {
		fmt.Println("\n🆕 First run detected")
		fmt.Println("📝 Running in FULL mode - collecting historical + current data")
		fmt.Printf("📅 Historical days: %d\n", DAYS_HISTORICAL)
	}

	fmt.Printf("⏱️  Rate limits: CG=%ds, CMC=%ds\n\n", int(CG_DELAY.Seconds()), int(CMC_DELAY.Seconds()))

	// Check API keys
	if CMC_API_KEY == "" {
		fmt.Println("⚠️  WARNING: CMC_API_KEY not set. Set it as environment variable:")
		fmt.Println("   export CMC_API_KEY='your-api-key-here'")
		fmt.Println("   Get your key from: https://coinmarketcap.com/api/")
	}

	// Initialize CSV files if they don't exist
	if !cgExists {
		fmt.Println("🔧 Initializing CoinGecko CSV...")
		if err := initCoinGeckoCSV(CG_CSV_PATH); err != nil {
			log.Fatalf("Failed to init CoinGecko CSV: %v", err)
		}
		fmt.Println("✅ CoinGecko CSV initialized")
	}

	if !cmcExists {
		fmt.Println("🔧 Initializing CoinMarketCap CSV...")
		if err := initCMCCSV(CMC_CSV_PATH); err != nil {
			log.Fatalf("Failed to init CMC CSV: %v", err)
		}
		fmt.Println("✅ CoinMarketCap CSV initialized")
	}

	// Phase 1: Collect CoinGecko Historical Data (only on first run)
	if !SKIP_HISTORICAL {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("📈 PHASE 1: CoinGecko Historical Data Collection")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		collectCoinGeckoHistorical()
	} else {
		fmt.Println("\n⏭️  Skipping historical data (already collected)")
	}

	// Phase 2: Collect CoinGecko Current Data (runs every time)
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 PHASE 2: CoinGecko Current Market Data")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	collectCoinGeckoCurrent()

	// Phase 3: Collect CoinMarketCap Data (runs every time)
	if CMC_API_KEY != "" {
		fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		fmt.Println("💰 PHASE 3: CoinMarketCap Market Data")
		fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
		collectCoinMarketCapData()
	} else {
		fmt.Println("\n⚠️  Skipping CoinMarketCap collection (API key not set)")
	}

	elapsed := time.Since(startTime)
	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║              COLLECTION COMPLETE ✅                ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Printf("⏱️  Total time: %v\n", elapsed.Round(time.Second))
	fmt.Printf("📊 Data saved to:\n")
	fmt.Printf("   - %s\n", CG_CSV_PATH)
	fmt.Printf("   - %s\n", CMC_CSV_PATH)

	if SKIP_HISTORICAL {
		fmt.Println("\n💡 Current snapshots added! Run again anytime to collect more data.")
		fmt.Println("📈 Tip: Schedule this with cron for continuous data collection:")
		fmt.Println("   */15 * * * * cd /path/to/api && go run main.go  # Every 15 minutes")
	} else {
		fmt.Println("\n💡 First collection complete! Historical data saved.")
		fmt.Println("📈 Run again to append new current snapshots (historical won't re-collect).")
	}
}

// ===== UTILITY FUNCTIONS =====
func fileExists(filepath string) bool {
	info, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// ===== COINGECKO HISTORICAL DATA =====
func collectCoinGeckoHistorical() {
	totalRecords := 0

	for i, tokenID := range TOKENS {
		fmt.Printf("\n[%d/%d] Collecting historical data for %s...\n", i+1, len(TOKENS), tokenID)

		url := fmt.Sprintf("https://api.coingecko.com/api/v3/coins/%s/market_chart?vs_currency=usd&days=%d&interval=daily",
			tokenID, DAYS_HISTORICAL)

		if COINGECKO_API_KEY != "" {
			url += "&x_cg_demo_api_key=" + COINGECKO_API_KEY
		}

		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error fetching %s: %v", tokenID, err)
			fmt.Printf("  ❌ Error: %v\n", err)
			time.Sleep(CG_DELAY)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("API error for %s: Status %d, Body: %s", tokenID, resp.StatusCode, string(body))
			fmt.Printf("  ❌ API Error: Status %d\n", resp.StatusCode)
			time.Sleep(CG_DELAY)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading body for %s: %v", tokenID, err)
			fmt.Printf("  ❌ Error reading response\n")
			time.Sleep(CG_DELAY)
			continue
		}

		var data CoinGeckoHistoricalResponse
		if err := json.Unmarshal(body, &data); err != nil {
			log.Printf("Error parsing JSON for %s: %v", tokenID, err)
			fmt.Printf("  ❌ Error parsing data\n")
			time.Sleep(CG_DELAY)
			continue
		}

		// Write to CSV
		count := writeCoinGeckoHistoricalToCSV(tokenID, &data)
		totalRecords += count
		fmt.Printf("  ✅ Collected %d historical records\n", count)

		// Rate limiting
		fmt.Printf("  ⏳ Waiting %ds (rate limit)...\n", int(CG_DELAY.Seconds()))
		time.Sleep(CG_DELAY)
	}

	fmt.Printf("\n📊 Total historical records collected: %d\n", totalRecords)
}

// ===== COINGECKO CURRENT DATA =====
func collectCoinGeckoCurrent() {
	// CoinGecko allows fetching multiple coins in one call (up to 250)
	batchSize := 50 // Conservative batch size
	totalRecords := 0

	for i := 0; i < len(TOKENS); i += batchSize {
		end := i + batchSize
		if end > len(TOKENS) {
			end = len(TOKENS)
		}
		batch := TOKENS[i:end]

		fmt.Printf("\n[Batch %d] Fetching current data for %d tokens...\n", (i/batchSize)+1, len(batch))

		ids := strings.Join(batch, ",")
		url := fmt.Sprintf("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=%s&order=market_cap_desc&sparkline=false&price_change_percentage=1h,24h,7d", ids)

		if COINGECKO_API_KEY != "" {
			url += "&x_cg_demo_api_key=" + COINGECKO_API_KEY
		}

		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error fetching batch: %v", err)
			fmt.Printf("  ❌ Error: %v\n", err)
			time.Sleep(CG_DELAY)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			log.Printf("API error: Status %d, Body: %s", resp.StatusCode, string(body))
			fmt.Printf("  ❌ API Error: Status %d\n", resp.StatusCode)
			time.Sleep(CG_DELAY)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		var data []CoinGeckoCurrentResponse
		if err := json.Unmarshal(body, &data); err != nil {
			log.Printf("Error parsing JSON: %v", err)
			fmt.Printf("  ❌ Error parsing data\n")
			time.Sleep(CG_DELAY)
			continue
		}

		count := writeCoinGeckoCurrentToCSV(&data)
		totalRecords += count
		fmt.Printf("  ✅ Collected %d current market records\n", count)

		fmt.Printf("  ⏳ Waiting %ds (rate limit)...\n", int(CG_DELAY.Seconds()))
		time.Sleep(CG_DELAY)
	}

	fmt.Printf("\n📊 Total current records collected: %d\n", totalRecords)
}

// ===== COINMARKETCAP DATA =====
func collectCoinMarketCapData() {
	if CMC_API_KEY == "" {
		return
	}

	// CMC uses symbols, not IDs like CoinGecko
	symbols := []string{"BTC", "ETH", "SOL", "ADA", "XRP", "DOT", "DOGE", "AVAX", "LINK", "MATIC",
		"UNI", "LTC", "XLM", "ATOM", "XMR", "TRX", "ETC", "FIL", "HBAR", "APT"}

	totalRecords := 0
	batchSize := 50 // CMC allows multiple symbols per call

	for i := 0; i < len(symbols); i += batchSize {
		end := i + batchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]

		fmt.Printf("\n[Batch %d] Fetching CMC data for %d tokens...\n", (i/batchSize)+1, len(batch))

		symbolStr := strings.Join(batch, ",")
		url := fmt.Sprintf("https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol=%s&convert=USD", symbolStr)

		client := &http.Client{}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			log.Printf("Error creating request: %v", err)
			fmt.Printf("  ❌ Error: %v\n", err)
			continue
		}

		req.Header.Set("X-CMC_PRO_API_KEY", CMC_API_KEY)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Error making request: %v", err)
			fmt.Printf("  ❌ Error: %v\n", err)
			time.Sleep(CMC_DELAY)
			continue
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		if resp.StatusCode != 200 {
			log.Printf("CMC API error: Status %d, Body: %s", resp.StatusCode, string(body))
			fmt.Printf("  ❌ API Error: Status %d\n", resp.StatusCode)
			time.Sleep(CMC_DELAY)
			continue
		}

		var data CMCQuoteResponse
		if err := json.Unmarshal(body, &data); err != nil {
			log.Printf("Error parsing CMC JSON: %v", err)
			fmt.Printf("  ❌ Error parsing data\n")
			time.Sleep(CMC_DELAY)
			continue
		}

		if data.Status.ErrorCode != 0 {
			log.Printf("CMC API returned error: %s", data.Status.ErrorMessage)
			fmt.Printf("  ❌ CMC Error: %s\n", data.Status.ErrorMessage)
			time.Sleep(CMC_DELAY)
			continue
		}

		count := writeCMCDataToCSV(&data)
		totalRecords += count
		fmt.Printf("  ✅ Collected %d CMC records\n", count)

		fmt.Printf("  ⏳ Waiting %ds (rate limit)...\n", int(CMC_DELAY.Seconds()))
		time.Sleep(CMC_DELAY)
	}

	fmt.Printf("\n📊 Total CMC records collected: %d\n", totalRecords)
}

// ===== CSV WRITERS =====
func initCoinGeckoCSV(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"timestamp", "date", "token_id", "symbol", "name",
		"price", "market_cap", "total_volume",
		"high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
		"circulating_supply", "total_supply", "ath", "ath_date", "source",
	}

	return writer.Write(headers)
}

func initCMCCSV(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"timestamp", "date", "symbol", "name", "slug",
		"price", "volume_24h", "volume_change_24h",
		"percent_change_1h", "percent_change_24h", "percent_change_7d",
		"market_cap", "market_cap_dominance",
		"circulating_supply", "total_supply", "max_supply",
		"last_updated", "source",
	}

	return writer.Write(headers)
}

func writeCoinGeckoHistoricalToCSV(tokenID string, data *CoinGeckoHistoricalResponse) int {
	file, err := os.OpenFile(CG_CSV_PATH, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Error opening CSV: %v", err)
		return 0
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	count := 0
	for i := 0; i < len(data.Prices); i++ {
		timestamp := int64(data.Prices[i][0] / 1000)
		date := time.Unix(timestamp, 0).Format("2006-01-02")
		price := data.Prices[i][1]

		var marketCap, volume float64
		if i < len(data.MarketCaps) {
			marketCap = data.MarketCaps[i][1]
		}
		if i < len(data.TotalVolumes) {
			volume = data.TotalVolumes[i][1]
		}

		record := []string{
			strconv.FormatInt(timestamp, 10),
			date,
			tokenID,
			"", // symbol (not in historical API)
			"", // name (not in historical API)
			fmt.Sprintf("%.8f", price),
			fmt.Sprintf("%.2f", marketCap),
			fmt.Sprintf("%.2f", volume),
			"", "", "", "", "", "", "", "", // empty fields for current data
			"coingecko_historical",
		}

		if err := writer.Write(record); err != nil {
			log.Printf("Error writing record: %v", err)
			continue
		}
		count++
	}

	return count
}

func writeCoinGeckoCurrentToCSV(data *[]CoinGeckoCurrentResponse) int {
	file, err := os.OpenFile(CG_CSV_PATH, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Error opening CSV: %v", err)
		return 0
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	timestamp := time.Now().Unix()
	date := time.Now().Format("2006-01-02")
	count := 0

	for _, coin := range *data {
		record := []string{
			strconv.FormatInt(timestamp, 10),
			date,
			coin.ID,
			coin.Symbol,
			coin.Name,
			fmt.Sprintf("%.8f", coin.CurrentPrice),
			fmt.Sprintf("%.2f", coin.MarketCap),
			fmt.Sprintf("%.2f", coin.TotalVolume),
			fmt.Sprintf("%.8f", coin.High24h),
			fmt.Sprintf("%.8f", coin.Low24h),
			fmt.Sprintf("%.8f", coin.PriceChange24h),
			fmt.Sprintf("%.4f", coin.PriceChangePercentage24h),
			fmt.Sprintf("%.2f", coin.CirculatingSupply),
			fmt.Sprintf("%.2f", coin.TotalSupply),
			fmt.Sprintf("%.8f", coin.ATH),
			coin.ATHDate,
			"coingecko_current",
		}

		if err := writer.Write(record); err != nil {
			log.Printf("Error writing record: %v", err)
			continue
		}
		count++
	}

	return count
}

func writeCMCDataToCSV(data *CMCQuoteResponse) int {
	file, err := os.OpenFile(CMC_CSV_PATH, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Printf("Error opening CSV: %v", err)
		return 0
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	timestamp := time.Now().Unix()
	date := time.Now().Format("2006-01-02")
	count := 0

	for _, coin := range data.Data {
		quote := coin.Quote["USD"]

		record := []string{
			strconv.FormatInt(timestamp, 10),
			date,
			coin.Symbol,
			coin.Name,
			coin.Slug,
			fmt.Sprintf("%.8f", quote.Price),
			fmt.Sprintf("%.2f", quote.Volume24h),
			fmt.Sprintf("%.4f", quote.VolumeChange24h),
			fmt.Sprintf("%.4f", quote.PercentChange1h),
			fmt.Sprintf("%.4f", quote.PercentChange24h),
			fmt.Sprintf("%.4f", quote.PercentChange7d),
			fmt.Sprintf("%.2f", quote.MarketCap),
			fmt.Sprintf("%.4f", quote.MarketCapDominance),
			fmt.Sprintf("%.2f", coin.CirculatingSupply),
			fmt.Sprintf("%.2f", coin.TotalSupply),
			fmt.Sprintf("%.2f", coin.MaxSupply),
			quote.LastUpdated,
			"coinmarketcap",
		}

		if err := writer.Write(record); err != nil {
			log.Printf("Error writing record: %v", err)
			continue
		}
		count++
	}

	return count
}