package org.Longshanks;

import org.Longshanks.MetricsCalculator;
import org.Longshanks.MetricsReporter;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Main entry point for the Risk-Reduced Momentum Backtest.
 *
 * Flow:
 * 1. Fetch data (if needed) via DataFetcher—checks DB existence AND sufficient data (min rows, date coverage).
 * 2. Run MomentumStrategy to get monthly returns.
 * 3. Compute metrics via MetricsCalculator and print report.
 *
 * Usage: Run Main.main(null) or from IDE.
 * Requires: sqlite-jdbc.jar in classpath.
 * DB: Creates/updates "prices.db".
 */
public class Main {

    private static final String DB_PATH = "prices.db";
    private static final LocalDate STRATEGY_START = LocalDate.parse("2005-01-01");
    private static final LocalDate STRATEGY_END = LocalDate.parse("2025-10-19");
    private static final double RISK_FREE_RATE = 0.02;
    private static final int MIN_ROWS_THRESHOLD = 100000;  // Rough min: ~500 tickers * ~5000 days / 2 (partial) ~250k, but conservative

    public static void main(String[] args) {
        System.out.println("Starting Risk-Reduced Momentum Backtest...");

        try {

            // Step 1: Check if DB exists and has sufficient data; fetch only if not
            if (hasSufficientData(DB_PATH)) {
                System.out.println("DB has sufficient data, skipping fetch.");
            } else {
                System.out.println("DB insufficient or missing. Fetching data...");
                // Optional: Delete empty DB to start fresh
                new File(DB_PATH).delete();
                DataFetcher fetcher = new DataFetcher();
                fetcher.fetchAndSaveToDB(DB_PATH);
                // Re-check after fetch (in case partial failure)
                if (!hasSufficientData(DB_PATH)) {
                    System.err.println("Fetch incomplete—DB still insufficient. Manual intervention needed.");
                    return;
                }
            }

            // Step 2: Execute strategy
            MomentumStrategy strategy = new MomentumStrategy(STRATEGY_START, STRATEGY_END);
            Map<LocalDate, Double> monthlyReturns = strategy.execute(DB_PATH);

            if (monthlyReturns.isEmpty()) {
                System.err.println("No strategy returns computed. Exiting.");
                return;
            }

            // Step 3: Compute and print metrics
            MetricsCalculator calculator = new MetricsCalculator(RISK_FREE_RATE, STRATEGY_START, STRATEGY_END);
            MetricsCalculator.MetricsReport report = calculator.compute(monthlyReturns, DB_PATH);
            MetricsReporter.print(report);

            System.out.println("Backtest complete.");

        } catch (Exception e) {
            System.err.println("Error in backtest: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Checks if DB exists and has sufficient data (row count > threshold AND date coverage).
     */
    private static boolean hasSufficientData(String dbPath) {
        File dbFile = new File(dbPath);
        if (!dbFile.exists()) {
            return false;
        }

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {

            // Check total rows
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS row_count FROM prices");
            rs.next();
            int rowCount = rs.getInt("row_count");
            if (rowCount < MIN_ROWS_THRESHOLD) {
                System.out.println("DB has only " + rowCount + " rows (threshold: " + MIN_ROWS_THRESHOLD + ")—refetching.");
                return false;
            }

            // Check date coverage (min/max dates)
            rs = stmt.executeQuery("SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM prices");
            rs.next();
            String minDateStr = rs.getString("min_date");
            String maxDateStr = rs.getString("max_date");
            if (minDateStr == null || maxDateStr == null) {
                return false;
            }

            LocalDate minDate = LocalDate.parse(minDateStr);
            LocalDate maxDate = LocalDate.parse(maxDateStr);
            boolean coversStart = !minDate.isAfter(STRATEGY_START);
            boolean coversEnd = !maxDate.isBefore(STRATEGY_END);

            if (!coversStart || !coversEnd) {
                System.out.println("DB dates (" + minDate + " to " + maxDate + ") don't fully cover [" + STRATEGY_START + ", " + STRATEGY_END + "]—refetching.");
                return false;
            }

            System.out.println("DB validated: " + rowCount + " rows, dates " + minDate + " to " + maxDate);
            return true;

        } catch (SQLException e) {
            System.err.println("DB validation error: " + e.getMessage() + ". Treating as insufficient.");
            return false;
        }
    }
}