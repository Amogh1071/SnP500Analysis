package org.Longshanks;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DataFetcher {

    private static final String START_DATE_STR = "2000-01-01";
    private static final String END_DATE_STR = "2025-10-19";
    private static final int BATCH_SIZE = 50;
    private static final int MAX_RETRIES = 3;
    private static final long BASE_SLEEP_MS = 5000;  // 5 seconds base delay
    private static final long RETRY_BACKOFF_MS = 10000;  // 10s exponential multiplier

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

    /**
     * Fetches stock prices from Yahoo Finance and saves to SQLite DB.
     * Table: prices (date TEXT PRIMARY KEY, ticker TEXT, adj_close REAL).
     * If DB exists, appends new data (no overwrite).
     *
     * @param dbPath Path to SQLite DB file (e.g., "prices.db").
     */
    public void fetchAndSaveToDB(String dbPath) {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath)) {
            createTableIfNotExists(conn);

            List<String> tickerList = Arrays.asList(TICKERS);
            int successful = 0;
            for (int i = 0; i < tickerList.size(); i += BATCH_SIZE) {
                List<String> batch = tickerList.subList(i, Math.min(i + BATCH_SIZE, tickerList.size()));
                System.out.println("Fetching batch " + (i / BATCH_SIZE + 1) + " (" + batch.size() + " tickers)...");
                for (String ticker : batch) {
                    if (fetchAndInsertStockWithRetry(conn, ticker)) {
                        successful++;
                    }
                    // Sleep after each ticker to respect rate limits
                    try {
                        Thread.sleep(BASE_SLEEP_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
            System.out.println("Data fetch complete: " + successful + "/" + tickerList.size() + " tickers saved to " + dbPath);
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        }
    }

    private void createTableIfNotExists(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS prices (" +
                "date TEXT NOT NULL, " +
                "ticker TEXT NOT NULL, " +
                "adj_close REAL NOT NULL, " +
                "PRIMARY KEY (date, ticker)" +
                ")";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private boolean fetchAndInsertStockWithRetry(Connection conn, String ticker) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                Calendar from = Calendar.getInstance();
                from.setTime(Date.from(START_DATE.atStartOfDay(ZoneId.systemDefault()).toInstant()));
                Calendar to = Calendar.getInstance();
                to.setTime(Date.from(END_DATE.atStartOfDay(ZoneId.systemDefault()).toInstant()));
                Stock stock = YahooFinance.get(ticker);
                List<HistoricalQuote> history = stock.getHistory(from, to, Interval.DAILY);

                String sql = "INSERT OR IGNORE INTO prices (date, ticker, adj_close) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    int inserted = 0;
                    for (HistoricalQuote quote : history) {
                        LocalDate date = quote.getDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                        if (date.isAfter(START_DATE) && date.isBefore(END_DATE)) {
                            pstmt.setString(1, date.format(DATE_FORMATTER));
                            pstmt.setString(2, ticker);
                            pstmt.setDouble(3, quote.getAdjClose().doubleValue());
                            pstmt.addBatch();
                            inserted++;
                        }
                    }
                    pstmt.executeBatch();
                    if (inserted > 0) {
                        System.out.println("  + " + ticker + ": " + inserted + " days inserted");
                        return true;
                    }
                }
                return true;  // Success even if no new data
            } catch (IOException e) {
                if (isRateLimitError(e) && attempt < MAX_RETRIES) {
                    long sleepTime = BASE_SLEEP_MS * (long) Math.pow(2, attempt - 1) + (long) (Math.random() * RETRY_BACKOFF_MS);
                    System.err.println("  Rate limit for " + ticker + " (attempt " + attempt + "): " + e.getMessage() + ". Retrying in " + (sleepTime / 1000) + "s...");
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                } else {
                    System.err.println("Error fetching " + ticker + " (attempt " + attempt + "): " + e.getMessage());
                    if (attempt == MAX_RETRIES) return false;
                }
            } catch (SQLException e) {
                System.err.println("SQL error for " + ticker + ": " + e.getMessage());
                return false;
            }
        }
        return false;
    }

    private boolean isRateLimitError(IOException e) {
        return e.getMessage() != null && e.getMessage().contains("429");
    }

    // Example usage in main module: DataFetcher fetcher = new DataFetcher(); fetcher.fetchAndSaveToDB("prices.db");
}