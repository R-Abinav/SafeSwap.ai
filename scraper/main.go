package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/playwright-community/playwright-go"
)

// ===== CONFIGURATION =====
var (
	// File paths
	CMC_CSV_PATH = "./data/crypto_data_coinmarketcap.csv"
	LOG_PATH     = "./data/scraper.log"

	// Tokens to scrape (CoinMarketCap slugs)
	TOKENS = []string{
		"bitcoin", "ethereum", "solana", "cardano", "ripple",
		"polkadot", "dogecoin", "avalanche-2012", "chainlink", "polygon",
		"uniswap", "litecoin", "stellar", "cosmos", "monero",
		"tron", "ethereum-classic", "filecoin", "internet-computer", "aptos",
		"shiba-inu", "wrapped-bitcoin", "dai", "leo-token", "toncoin",
	}

	// Historical data range (in days)
	DAYS_HISTORICAL = 365 // Get 1 year of data

	// Scraping delay (to avoid rate limiting)
	SCRAPE_DELAY = 3 * time.Second
)

// ===== DATA STRUCTURES =====
type HistoricalData struct {
	Date              string
	TokenSymbol       string
	TokenName         string
	Open              float64
	High              float64
	Low               float64
	Close             float64
	Volume            float64
	MarketCap         float64
	Source            string
}

func main() {
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
	fmt.Println("║   COINMARKETCAP HISTORICAL DATA SCRAPER v2.0      ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Printf("\n📊 Collecting historical data for %d tokens\n", len(TOKENS))
	fmt.Printf("📁 Output file: %s\n", CMC_CSV_PATH)
	fmt.Printf("📅 Historical days: %d\n", DAYS_HISTORICAL)
	fmt.Printf("⏱️  Delay between tokens: %ds\n\n", int(SCRAPE_DELAY.Seconds()))

	// Initialize CSV file
	fmt.Println("🔧 Initializing CSV file...")
	if err := initCSV(CMC_CSV_PATH); err != nil {
		log.Fatalf("Failed to init CSV: %v", err)
	}
	fmt.Println("✅ CSV file initialized")

	// Install Playwright (only needed first time)
	fmt.Println("🎭 Initializing Playwright...")
	err = playwright.Install()
	if err != nil {
		log.Fatalf("Could not install playwright: %v", err)
	}
	fmt.Println("✅ Playwright ready")

	// Start scraping
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("🚀 Starting Historical Data Collection")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	
	totalRecords := scrapeHistoricalData()

	elapsed := time.Since(startTime)
	fmt.Println("\n╔════════════════════════════════════════════════════╗")
	fmt.Println("║              SCRAPING COMPLETE ✅                  ║")
	fmt.Println("╚════════════════════════════════════════════════════╝")
	fmt.Printf("📊 Total records collected: %d\n", totalRecords)
	fmt.Printf("⏱️  Total time: %v\n", elapsed.Round(time.Second))
	fmt.Printf("📁 Data saved to: %s\n", CMC_CSV_PATH)
	fmt.Printf("📈 Average: %.1f records per token\n", float64(totalRecords)/float64(len(TOKENS)))
}

func initCSV(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"date",
		"token_symbol",
		"token_name",
		"open",
		"high",
		"low",
		"close",
		"volume",
		"market_cap",
		"source",
	}

	return writer.Write(headers)
}

func scrapeHistoricalData() int {
	totalRecords := 0

	// Start Playwright
	pw, err := playwright.Run()
	if err != nil {
		log.Fatalf("Could not start playwright: %v", err)
	}
	defer pw.Stop()

	// Launch browser once for all tokens (more efficient)
	fmt.Println("🌐 Launching headless browser...")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(true),
	})
	if err != nil {
		log.Fatalf("Could not launch browser: %v", err)
	}
	defer browser.Close()
	fmt.Println("✅ Browser launched")

	// Calculate date range
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -DAYS_HISTORICAL)
	startStr := startDate.Format("20060102")
	endStr := endDate.Format("20060102")

	// Scrape each token
	for i, token := range TOKENS {
		fmt.Printf("\n[%d/%d] 🔍 Scraping %s...\n", i+1, len(TOKENS), strings.ToUpper(token))
		
		records := scrapeToken(browser, token, startStr, endStr)
		
		if len(records) > 0 {
			if err := appendToCSV(CMC_CSV_PATH, records); err != nil {
				log.Printf("Error writing data for %s: %v", token, err)
				fmt.Printf("  ❌ Error writing to CSV\n")
			} else {
				totalRecords += len(records)
				fmt.Printf("  ✅ Collected %d records\n", len(records))
			}
		} else {
			fmt.Printf("  ⚠️  No data collected\n")
		}

		// Rate limiting delay (except for last token)
		if i < len(TOKENS)-1 {
			fmt.Printf("  ⏳ Waiting %ds...\n", int(SCRAPE_DELAY.Seconds()))
			time.Sleep(SCRAPE_DELAY)
		}
	}

	return totalRecords
}

func scrapeToken(browser playwright.Browser, token, startDate, endDate string) []HistoricalData {
	var records []HistoricalData

	// Create a new page
	page, err := browser.NewPage()
	if err != nil {
		log.Printf("Could not create page: %v", err)
		return records
	}
	defer page.Close()

	// Build URL
	url := fmt.Sprintf("https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s",
		token, startDate, endDate)

	// Navigate to the page
	_, err = page.Goto(url, playwright.PageGotoOptions{
		WaitUntil: playwright.WaitUntilStateNetworkidle,
		Timeout:   playwright.Float(45000), // 45 second timeout
	})
	if err != nil {
		log.Printf("Could not goto page for %s: %v", token, err)
		fmt.Printf("  ⚠️  Page load timeout\n")
		return records
	}

	// Wait a bit for content to load
	time.Sleep(2 * time.Second)

	// Try to find the table
	rows, err := page.Locator("table tbody tr").All()
	if err != nil || len(rows) == 0 {
		log.Printf("No table rows found for %s", token)
		return records
	}

	fmt.Printf("  📊 Found %d rows\n", len(rows))

	// Extract data from each row
	for _, row := range rows {
		cells, err := row.Locator("td").All()
		if err != nil || len(cells) < 7 {
			continue
		}

		data := HistoricalData{
			TokenSymbol: strings.ToUpper(token),
			TokenName:   "",
			Source:      "CoinMarketCap",
		}

		// Extract date (column 0)
		dateText, _ := cells[0].TextContent()
		data.Date = parseDate(strings.TrimSpace(dateText))

		// Extract OHLC and Volume
		data.Open = parsePrice(cells, 1)
		data.High = parsePrice(cells, 2)
		data.Low = parsePrice(cells, 3)
		data.Close = parsePrice(cells, 4)
		data.Volume = parsePrice(cells, 5)
		data.MarketCap = parsePrice(cells, 6)

		// Only add if we have valid data
		if data.Date != "" && data.Close > 0 {
			records = append(records, data)
		}
	}

	return records
}

func parsePrice(cells []playwright.Locator, index int) float64 {
	if index >= len(cells) {
		return 0
	}

	text, err := cells[index].TextContent()
	if err != nil {
		return 0
	}

	// Clean the string
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "$", "")
	text = strings.ReplaceAll(text, ",", "")

	// Handle billions/millions
	multiplier := 1.0
	if strings.Contains(text, "B") || strings.Contains(text, "b") {
		multiplier = 1e9
		text = strings.ReplaceAll(text, "B", "")
		text = strings.ReplaceAll(text, "b", "")
	} else if strings.Contains(text, "M") || strings.Contains(text, "m") {
		multiplier = 1e6
		text = strings.ReplaceAll(text, "M", "")
		text = strings.ReplaceAll(text, "m", "")
	}

	text = strings.TrimSpace(text)

	if val, err := strconv.ParseFloat(text, 64); err == nil {
		return val * multiplier
	}

	return 0
}

func parseDate(dateStr string) string {
	// Try multiple date formats
	formats := []string{
		"Jan 02, 2006",
		"January 02, 2006",
		"02-01-2006",
		"2006-01-02",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}

	return dateStr
}

func appendToCSV(filepath string, data []HistoricalData) error {
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, d := range data {
		record := []string{
			d.Date,
			d.TokenSymbol,
			d.TokenName,
			fmt.Sprintf("%.8f", d.Open),
			fmt.Sprintf("%.8f", d.High),
			fmt.Sprintf("%.8f", d.Low),
			fmt.Sprintf("%.8f", d.Close),
			fmt.Sprintf("%.2f", d.Volume),
			fmt.Sprintf("%.2f", d.MarketCap),
			d.Source,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}