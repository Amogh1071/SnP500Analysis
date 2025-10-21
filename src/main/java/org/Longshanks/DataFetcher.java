package org.Longshanks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataFetcher {

    private static final String START_DATE_STR = "2000-01-03";  // Aligned with strategy start
    private static final String END_DATE_STR = "2025-10-17";
    private static final int BATCH_SIZE = 50;
    private static final int MAX_RETRIES = 3;
    private static final long BASE_SLEEP_MS = 2000;  // 2 seconds after each ticker (matches Python rate limit)
    private static final long RETRY_BACKOFF_MS = 5000;  // 5s base for retries on errors

    private static final String[] TICKERS = {
            "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "AVGO", "TSLA", "BRK-B",
            "WMT", "JPM", "UNH", "V", "MA", "PG", "JNJ", "XOM", "HD", "CVX", "KO", "ABBV", "MRK", "BAC", "COST",
            "NFLX", "AMD", "CRM", "ABT", "TMO", "ACN", "DHR", "TXN", "PM", "AXP", "NEE", "CAT", "GS", "AMGN", "LLY",
            "LIN", "PFE", "LOW", "ADBE", "COF", "BMY", "RTX", "MDT", "HON", "UNP", "SBUX", "DIS", "CSCO", "VZ",
            "ORCL", "IBM", "INTC", "QCOM", "AMAT", "LRCX", "NOW", "ADSK", "PANW", "GILD", "SYK", "MU", "SPGI", "ETN",
            "KLAC", "BSX", "DE", "PGR", "ELV", "SO", "VRTX", "REGN", "CMCSA", "COP", "TJX", "ISRG", "APH", "MDLZ",
            "T", "BLK", "INTU", "SCHW", "BKNG", "NOC", "GD", "WM", "SHW", "RCL", "MMM", "CI", "ECL", "HWM", "AON",
            "MSI", "CTAS", "BK", "UPS", "EMR", "ITW", "GLW", "AJG", "TDG", "JCI", "USB", "MAR", "PNC", "APO", "RSG",
            "MNST", "CSX", "VST", "AZO", "FI", "TEL", "NSC", "PWR", "PYPL", "CL", "FTNT", "ZTS", "URI", "COR", "AEP",
            "WDAY", "HLT", "KMI", "DLR", "SRE", "FCX", "TRV", "SPG", "EOG", "AFL", "CMI", "APD", "CMG", "FDX", "MPC",
            "GM", "TFC", "O", "ROP", "BDX", "PSA", "NXPI", "DDOG", "LHX", "AXON", "PSX", "MET", "D", "ALL", "ROST",
            "IDXX", "NDAQ", "EA", "PCAR", "VLO", "FAST", "SLB", "EXC", "TTWO", "MPWR", "STX", "XEL", "GRMN", "CARR",
            "F", "CBRE", "DHI", "KR", "GWW", "PAYX", "WBD", "BKR", "AMP", "EW", "WDC", "CPRT", "OKE", "AIG", "AME",
            "ETR", "CCI", "PEG", "EBAY", "CTVA", "MSCI", "TGT", "YUM", "KMB", "FANG", "OXY", "A", "RMD", "ROK", "VMC",
            "DAL", "FICO", "SYY", "KDP", "HSY", "MLM", "WEC", "CCL", "CAH", "ED", "EL", "PCG", "LYV", "OTIS", "XYL",
            "PRU", "MCHP", "HIG", "FIS", "IQV", "GEHC", "EQT", "LVS", "DD", "WAB", "HUM", "VICI", "NRG", "VRSK", "ACGL",
            "CTSH", "WTW", "RJF", "EXR", "UAL", "VTR", "LEN", "TRGP", "STT", "CSGP", "SMCI", "IRM", "EME", "IR", "ADM",
            "HPE", "NUE", "KHC", "IBKR", "DTE", "KVUE", "TSCO", "AWK", "ODFL", "BRO", "K", "AEE", "ATO", "MTB", "WRB",
            "EFX", "KEYS", "MTD", "PPL", "FE", "ROL", "FITB", "BR", "AVB", "EXPE", "TDY", "DXCM", "CNP", "GIS", "SYF",
            "HPQ", "CBOE", "FSLR", "VRSN", "IP", "STZ", "PHM", "TTD", "PTC", "TPR", "EXE", "CINF", "ULTA", "NTRS", "NTAP",
            "LH", "EQR", "LDOS", "STE", "DG", "PPG", "DOV", "TROW", "HUBB", "HBAN", "WSM", "JBL", "PODD", "CMS", "TER",
            "DRI", "TYL", "EIX", "SW", "MH", "TPL", "CHD", "ON", "CFG", "DGX", "NVR", "SBAC", "RF", "STLD", "BIIB",
            "GPN", "L", "NI", "ZBH", "CPAY", "DVN", "CDW", "WAT", "RL", "LULU", "DLTR", "BG", "WST", "HAL", "AMCR",
            "TSN", "KEY", "J", "GPC", "TRMB", "LII", "PKG", "MKC", "APTV", "EVRG", "IT", "PNR", "GDDY", "SNA", "LUV",
            "PFG", "LNT", "INVH", "CTRA", "CNC", "FFIV", "INCY", "WY", "BBY", "ERIE", "FTV", "IFF", "GEN", "EXPD",
            "JBHT", "HOLX", "MAA", "DOW", "ALLE", "CHRW", "OMC", "ZBRA", "LYB", "DECK", "KIM", "EG", "CLX", "TXT",
            "COO", "MAS", "DPZ", "BF-B", "BLDR", "CF", "HRL", "BALL", "REG", "NDSN", "UHS", "DOC", "ARE", "IEX",
            "SOLV", "AVY", "WYNN", "FOX", "FOXA", "UDR", "BAX", "VTRS", "BEN", "PAYC", "BXP", "SJM", "HST", "SWKS",
            "GNRC", "PNW", "JKHY", "HII", "CPT", "GL", "ALB", "RVTY", "FDS", "POOL", "DAY", "AIZ", "SWK", "HAS",
            "AKAM", "NCLH", "MOH", "AES", "IVZ", "MRNA", "AOS", "NWSA", "IPG", "TECH", "ALGN", "TAP", "MOS", "CPB",
            "LW", "DVA", "CAG", "CRL", "MGM", "FRT", "APA", "EPAM", "MHK", "MTCH", "LKQ", "HSIC", "EMN", "KMX", "NWS"
    };

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final LocalDate START_DATE = LocalDate.parse(START_DATE_STR, DATE_FORMATTER);
    private static final LocalDate END_DATE = LocalDate.parse(END_DATE_STR, DATE_FORMATTER);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofSeconds(10))
            .build();

    /**
     * Fetches stock prices from Yahoo Finance v8 API and saves to CSV.
     * Mimics yfinance behavior: adjusted closes, daily interval, rate-limited.
     * Appends if CSV exists. Cleans low-coverage tickers (80% threshold) after fetch.
     * CSV format: date,ticker,adj_close (long format, no header on append).
     *
     * @param csvPath Path to CSV file (e.g., "stock_prices.csv").
     */
    public void fetchAndSaveToCSV(String csvPath) {
        boolean append = new File(csvPath).exists();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvPath, append))) {
            if (!append) {
                writer.write("date,ticker,adj_close\n");  // Header only on new file
            }

            List<String> tickerList = Arrays.asList(TICKERS);
            int successful = 0;
            for (int i = 0; i < tickerList.size(); i += BATCH_SIZE) {
                List<String> batch = tickerList.subList(i, Math.min(i + BATCH_SIZE, tickerList.size()));
                System.out.println("Fetching batch " + (i / BATCH_SIZE + 1) + " (" + batch.size() + " tickers)...");
                for (String ticker : batch) {
                    List<String> dataForTicker = fetchOneTickerWithRetry(ticker);
                    if (!dataForTicker.isEmpty()) {
                        for (String line : dataForTicker) {
                            writer.write(line + "\n");
                        }
                        successful++;
                    }
                    // Rate limit sleep after each ticker
                    try {
                        Thread.sleep(BASE_SLEEP_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                writer.flush();  // Flush after batch
            }

            // Clean low-coverage tickers after all fetches (matches Python's 80% threshold)
            // Note: This requires reading the full CSV back in—inefficient for large files, but simple.
            cleanLowCoverageTickers(csvPath, tickerList);

            System.out.println("Data fetch complete: " + successful + "/" + tickerList.size() + " tickers processed in " + csvPath);
        } catch (IOException e) {
            System.err.println("CSV write error: " + e.getMessage());
        }
    }

    private List<String> fetchOneTickerWithRetry(String ticker) {
        long startUnix = START_DATE.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        long endUnix = END_DATE.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        String urlStr = String.format("https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d", ticker, startUnix, endUnix);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(urlStr))
                        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 429) {
                    if (attempt < MAX_RETRIES) {
                        long sleepTime = RETRY_BACKOFF_MS * (long) Math.pow(2, attempt - 1) + (long) (Math.random() * 5000);
                        System.err.println("  Rate limit for " + ticker + " (attempt " + attempt + "): HTTP 429. Retrying in " + (sleepTime / 1000) + "s...");
                        Thread.sleep(sleepTime);
                        continue;
                    } else {
                        System.err.println("  Max retries exceeded for " + ticker + ": HTTP 429");
                        return List.of();
                    }
                } else if (response.statusCode() == 200) {
                    JsonNode root = mapper.readTree(response.body());
                    JsonNode result = root.path("chart").path("result").get(0);
                    if (result == null || result.isMissingNode()) {
                        System.out.println("  No data for " + ticker);
                        return List.of();
                    }

                    JsonNode timestampsNode = result.path("timestamp");
                    JsonNode indicatorsNode = result.path("indicators").path("quote").get(0);
                    JsonNode adjClosesNode = indicatorsNode.path("adjclose");

                    if (!timestampsNode.isArray() || timestampsNode.size() == 0) {
                        System.out.println("  Empty timestamps for " + ticker);
                        return List.of();
                    }

                    if (!adjClosesNode.isArray() || adjClosesNode.size() == 0) {
                        System.out.println("  Empty adjclose for " + ticker);
                        return List.of();
                    }

                    // Use the smaller size to avoid index out of bounds
                    int numPoints = Math.min(timestampsNode.size(), adjClosesNode.size());

                    java.util.List<String> lines = new java.util.ArrayList<>();
                    int inserted = 0;
                    for (int j = 0; j < numPoints; j++) {
                        long timestamp = timestampsNode.get(j).asLong(0L);
                        if (timestamp == 0) continue;

                        Instant instant = Instant.ofEpochSecond(timestamp);
                        LocalDate date = LocalDate.ofInstant(instant, ZoneId.systemDefault());
                        if (date.isBefore(START_DATE) || date.isAfter(END_DATE)) continue;

                        JsonNode adjCloseNode = adjClosesNode.get(j);
                        if (adjCloseNode == null || adjCloseNode.isNull()) continue;

                        double adjClose;
                        if (!adjCloseNode.isNumber()) {
                            System.err.println("  Non-numeric adjclose for " + ticker + " at index " + j + ": " + adjCloseNode);
                            continue;
                        }
                        adjClose = adjCloseNode.asDouble();
                        if (adjClose <= 0) continue;

                        String line = date.format(DATE_FORMATTER) + "," + ticker + "," + adjClose;
                        lines.add(line);
                        inserted++;
                    }
                    if (inserted > 0) {
                        System.out.println("  + " + ticker + ": " + inserted + " days fetched");
                    }
                    return lines;
                } else {
                    System.err.println("  HTTP " + response.statusCode() + " for " + ticker + " (attempt " + attempt + "): " + response.body().substring(0, Math.min(200, response.body().length())));
                    if (attempt == MAX_RETRIES) return List.of();
                }
            } catch (IOException | InterruptedException e) {
                System.err.println("  Error fetching " + ticker + " (attempt " + attempt + "): " + e.getMessage());
                if (attempt == MAX_RETRIES) return List.of();
                try {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);  // Progressive backoff on general errors
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return List.of();
                }
            }
        }
        return List.of();
    }

    private void cleanLowCoverageTickers(String csvPath, List<String> tickerList) {
        // Read full CSV into memory for cleaning (ok for ~2M rows)
        Set<String> allDates = new HashSet<>();
        java.util.Map<String, Integer> tickerCounts = new java.util.HashMap<>();
        java.util.List<String> validLines = new java.util.ArrayList<>();

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            String line;
            boolean headerSkipped = false;
            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;  // Skip header
                }
                String[] parts = line.split(",", 3);  // Limit split to avoid issues in prices
                if (parts.length < 3) continue;

                try {
                    LocalDate date = LocalDate.parse(parts[0], formatter);
                    String ticker = parts[1];
                    double price = Double.parseDouble(parts[2]);

                    if (price > 0 && date.isAfter(START_DATE.minusDays(1)) && date.isBefore(END_DATE.plusDays(1))) {
                        allDates.add(parts[0]);
                        tickerCounts.put(ticker, tickerCounts.getOrDefault(ticker, 0) + 1);
                        validLines.add(line);
                    }
                } catch (Exception ignored) {
                    // Skip invalid
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading CSV for cleaning: " + e.getMessage());
            return;
        }

        int totalDays = allDates.size();
        if (totalDays == 0) return;

        int minThreshold = (int) (0.8 * totalDays);
        System.out.println("Cleaning: threshold " + minThreshold + "/" + totalDays + " days (80%)");

        // Filter validLines to keep only good tickers
        java.util.List<String> keptLines = new java.util.ArrayList<>();
        keptLines.add("date,ticker,adj_close");  // Re-add header
        int removed = 0;
        for (String line : validLines) {
            String[] parts = line.split(",", 3);
            String ticker = parts[1];
            int coverage = tickerCounts.getOrDefault(ticker, 0);
            if (coverage >= minThreshold) {
                keptLines.add(line);
            } else if (coverage > 0) {
                System.out.println("  Removed low coverage: " + ticker + " (" + coverage + "/" + totalDays + " days)");
                removed++;
            }
        }

        // Rewrite CSV
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvPath))) {
            for (String keptLine : keptLines) {
                writer.write(keptLine + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error rewriting CSV: " + e.getMessage());
        }

        if (removed == 0) {
            System.out.println("  No low-coverage tickers found.");
        } else {
            System.out.println("  Removed " + removed + " low-coverage tickers.");
        }
    }
}