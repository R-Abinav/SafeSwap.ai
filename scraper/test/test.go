package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/playwright-community/playwright-go"
)

// Test configuration
var TEST_TOKENS = []string{"bitcoin", "ethereum"} // Just test 2 tokens
var TEST_LOG_PATH = "./test_scraper.log"

func main() {
	startTime := time.Now()
	
	// Setup logging
	logFile, err := os.OpenFile(TEST_LOG_PATH, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	fmt.Println("=== Crypto Scraper Test Script (Playwright) ===")
	fmt.Println("Testing CoinMarketCap and CoinGecko with headless browser...")
	fmt.Printf("Log file: %s\n", TEST_LOG_PATH)
	fmt.Println("This will run for ~2 minutes with detailed logs")

	// Install Playwright (only needed first time)
	fmt.Println("Initializing Playwright...")
	err = playwright.Install()
	if err != nil {
		log.Fatalf("Could not install playwright: %v", err)
	}

	// Test 1: CoinMarketCap Historical Data
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("TEST 1: CoinMarketCap Historical Data")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	testCoinMarketCap()

	// Small delay between tests
	time.Sleep(3 * time.Second)

	// Test 2: CoinGecko Current Data
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("TEST 2: CoinGecko Current Data")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	testCoinGecko()

	elapsed := time.Since(startTime)
	fmt.Println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("Test Complete! Time taken: %v\n", elapsed.Round(time.Second))
	fmt.Printf("Check %s for detailed logs\n", TEST_LOG_PATH)
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
}

func testCoinMarketCap() {
	fmt.Println("\n[CMC Test] Starting CoinMarketCap scrape test...")
	
	// Start Playwright
	pw, err := playwright.Run()
	if err != nil {
		log.Fatalf("Could not start playwright: %v", err)
	}
	defer pw.Stop()

	// Launch browser
	fmt.Println("[CMC Test] Launching headless browser...")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(true),
	})
	if err != nil {
		log.Fatalf("Could not launch browser: %v", err)
	}
	defer browser.Close()

	// Calculate date range (last 7 days for testing)
	endDate := time.Now()
	startDate := endDate.AddDate(0, 0, -7)
	
	for _, token := range TEST_TOKENS {
		fmt.Printf("\n[CMC Test] Testing %s...\n", strings.ToUpper(token))
		
		// Create a new page
		page, err := browser.NewPage()
		if err != nil {
			log.Printf("[CMC Test] Could not create page: %v", err)
			continue
		}
		
		startStr := startDate.Format("20060102")
		endStr := endDate.Format("20060102")
		url := fmt.Sprintf("https://coinmarketcap.com/currencies/%s/historical-data/?start=%s&end=%s", 
			token, startStr, endStr)
		
		fmt.Printf("[CMC Test] URL: %s\n", url)
		fmt.Println("[CMC Test] Loading page...")
		
		// Navigate to the page
		if _, err = page.Goto(url, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateNetworkidle,
			Timeout:   playwright.Float(30000), // 30 second timeout
		}); err != nil {
			log.Printf("[CMC Test] Could not goto page: %v", err)
			page.Close()
			continue
		}
		
		fmt.Println("[CMC Test] ✓ Page loaded, waiting for content...")
		
		// Wait for the table to be visible
		time.Sleep(3 * time.Second)
		
		// Try multiple selectors for the historical data table
		selectors := []string{
			"table tbody tr",
			"table tr",
			"[class*='historical'] table tr",
			"div[class*='table'] tr",
		}
		
		rowCount := 0
		dataFound := false
		
		for _, selector := range selectors {
			rows, err := page.Locator(selector).All()
			if err == nil && len(rows) > 0 {
				fmt.Printf("[CMC Test] ✓ Found %d rows with selector: %s\n", len(rows), selector)
				dataFound = true
				rowCount = len(rows)
				
				// Extract data from first 3 rows
				for i := 0; i < min(3, len(rows)); i++ {
					rowText, _ := rows[i].TextContent()
					fmt.Printf("[CMC Test] Row %d text: %s\n", i+1, strings.TrimSpace(rowText)[:min(100, len(rowText))])
					
					// Try to extract specific cells
					cells, err := rows[i].Locator("td").All()
					if err == nil && len(cells) > 0 {
						fmt.Printf("[CMC Test]   → Found %d cells\n", len(cells))
						if len(cells) >= 5 {
							date, _ := cells[0].TextContent()
							open, _ := cells[1].TextContent()
							close, _ := cells[4].TextContent()
							fmt.Printf("[CMC Test]   → Date: '%s', Open: '%s', Close: '%s'\n", 
								strings.TrimSpace(date), strings.TrimSpace(open), strings.TrimSpace(close))
						}
					}
				}
				break
			}
		}
		
		// Summary
		fmt.Println("[CMC Test] ─────────────────────────────")
		fmt.Printf("[CMC Test] Summary for %s:\n", strings.ToUpper(token))
		fmt.Printf("[CMC Test]   - Table Found: %v\n", dataFound)
		fmt.Printf("[CMC Test]   - Rows Found: %d\n", rowCount)
		if rowCount == 0 {
			fmt.Println("[CMC Test]   - ❌ NO DATA SCRAPED")
			// Get page content for debugging
			content, _ := page.Content()
			fmt.Printf("[CMC Test]   - Page HTML length: %d bytes\n", len(content))
		} else {
			fmt.Println("[CMC Test]   - ✅ DATA SUCCESSFULLY SCRAPED!")
		}
		fmt.Println("[CMC Test] ─────────────────────────────")
		
		page.Close()
		time.Sleep(2 * time.Second)
	}
}

func testCoinGecko() {
	fmt.Println("\n[CG Test] Starting CoinGecko scrape test...")
	
	// Start Playwright
	pw, err := playwright.Run()
	if err != nil {
		log.Fatalf("Could not start playwright: %v", err)
	}
	defer pw.Stop()

	// Launch browser
	fmt.Println("[CG Test] Launching headless browser...")
	browser, err := pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(true),
	})
	if err != nil {
		log.Fatalf("Could not launch browser: %v", err)
	}
	defer browser.Close()
	
	for _, token := range TEST_TOKENS {
		fmt.Printf("\n[CG Test] Testing %s...\n", strings.ToUpper(token))
		
		// Create a new page
		page, err := browser.NewPage()
		if err != nil {
			log.Printf("[CG Test] Could not create page: %v", err)
			continue
		}
		
		url := fmt.Sprintf("https://www.coingecko.com/en/coins/%s", token)
		fmt.Printf("[CG Test] URL: %s\n", url)
		fmt.Println("[CG Test] Loading page...")
		
		// Navigate to the page
		if _, err = page.Goto(url, playwright.PageGotoOptions{
			WaitUntil: playwright.WaitUntilStateNetworkidle,
			Timeout:   playwright.Float(30000),
		}); err != nil {
			log.Printf("[CG Test] Could not goto page: %v", err)
			page.Close()
			continue
		}
		
		fmt.Println("[CG Test] ✓ Page loaded, waiting for content...")
		time.Sleep(3 * time.Second)
		
		// Try multiple selectors for price
		priceSelectors := []string{
			"span.no-wrap",
			"[data-price-btc]",
			"span[class*='price']",
			"div[class*='price'] span",
			"[data-target='price.price']",
		}
		
		priceFound := false
		var priceText string
		
		for _, selector := range priceSelectors {
			elements, err := page.Locator(selector).All()
			if err == nil && len(elements) > 0 {
				for _, elem := range elements {
					text, _ := elem.TextContent()
					text = strings.TrimSpace(text)
					if strings.Contains(text, "$") && len(text) > 0 {
						fmt.Printf("[CG Test] ✓ Found price with selector '%s': %s\n", selector, text)
						priceText = text
						priceFound = true
						break
					}
				}
				if priceFound {
					break
				}
			}
		}
		
		// Try to get market cap and volume
		marketCapFound := false
		volumeFound := false
		
		// Market cap selectors
		mcSelectors := []string{
			"[data-target='price.marketCap']",
			"span[class*='market-cap']",
			"div[class*='market-cap'] span",
		}
		
		for _, selector := range mcSelectors {
			elem := page.Locator(selector).First()
			if elem != nil {
				text, err := elem.TextContent()
				if err == nil && strings.Contains(text, "$") {
					fmt.Printf("[CG Test] ✓ Found market cap: %s\n", strings.TrimSpace(text))
					marketCapFound = true
					break
				}
			}
		}
		
		// Volume selectors
		volSelectors := []string{
			"[data-target='price.volume']",
			"span[class*='volume']",
			"div[class*='volume'] span",
		}
		
		for _, selector := range volSelectors {
			elem := page.Locator(selector).First()
			if elem != nil {
				text, err := elem.TextContent()
				if err == nil && strings.Contains(text, "$") {
					fmt.Printf("[CG Test] ✓ Found volume: %s\n", strings.TrimSpace(text))
					volumeFound = true
					break
				}
			}
		}
		
		// Summary
		fmt.Println("[CG Test] ─────────────────────────────")
		fmt.Printf("[CG Test] Summary for %s:\n", strings.ToUpper(token))
		fmt.Printf("[CG Test]   - Price Found: %v", priceFound)
		if priceFound {
			fmt.Printf(" (%s)\n", priceText)
		} else {
			fmt.Println()
		}
		fmt.Printf("[CG Test]   - Market Cap Found: %v\n", marketCapFound)
		fmt.Printf("[CG Test]   - Volume Found: %v\n", volumeFound)
		
		if !priceFound {
			fmt.Println("[CG Test]   - ❌ NO PRICE DATA FOUND")
			content, _ := page.Content()
			fmt.Printf("[CG Test]   - Page HTML length: %d bytes\n", len(content))
		} else {
			fmt.Println("[CG Test]   - ✅ DATA SUCCESSFULLY SCRAPED!")
		}
		fmt.Println("[CG Test] ─────────────────────────────")
		
		page.Close()
		time.Sleep(2 * time.Second)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}