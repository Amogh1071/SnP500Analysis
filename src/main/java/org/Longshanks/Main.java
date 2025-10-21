package org.Longshanks;

import org.Longshanks.MetricsCalculator;
import org.Longshanks.MetricsReporter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Main entry point for the Risk-Reduced Momentum Backtest.
 *
 * Flow:
 * 1. Fetch data (if needed) via DataFetcher—checks CSV existence AND sufficient data (min rows POST-START, date coverage).
 * 2. Run MomentumStrategy to get monthly returns (assumes strategy updated to read CSV).
 * 3. Compute metrics via MetricsCalculator and print report (assumes calculator updated to read CSV if needed).
 *
 * Usage: Run Main.main(null) or from IDE.
 * CSV: Creates/updates "stock_prices.csv" in long format: date,ticker,adj_close
 */
public class Main {

    private static final String CSV_PATH = "stock_prices.csv";
    private static final LocalDate STRATEGY_START = LocalDate.parse("2005-01-03");

    private static final LocalDate STRATEGY_END = LocalDate.parse("2025-10-17");
    private static final double RISK_FREE_RATE = 0.02;
    private static final int MIN_ROWS_THRESHOLD = 1000;  // Min post-2005 rows: ~500 tickers * 5000 days / 10 (partial) ~250k, but conservative for full

    public static void main(String[] args) {
        System.out.println("Starting Risk-Reduced Momentum Backtest...");

        try {

            // Step 1: Check if CSV exists and has sufficient data; fetch only if not
            if (hasSufficientData(CSV_PATH)) {
                System.out.println("CSV has sufficient data, skipping fetch.");
            } else {
                System.out.println("CSV insufficient or missing. Fetching data...");
                // Optional: Delete empty CSV to start fresh
                new File(CSV_PATH).delete();
                DataFetcher fetcher = new DataFetcher();
                fetcher.fetchAndSaveToCSV(CSV_PATH);
                // Re-check after fetch (in case partial failure)
                if (!hasSufficientData(CSV_PATH)) {
                    System.err.println("Fetch incomplete—CSV still insufficient. Manual intervention needed.");
                    return;
                }
            }

            // Step 2: Execute strategy (update MomentumStrategy to read CSV)
            MomentumStrategy strategy = new MomentumStrategy(STRATEGY_START, STRATEGY_END);
            Map<LocalDate, Double> monthlyReturns = strategy.execute(CSV_PATH);

            if (monthlyReturns.isEmpty()) {
                System.err.println("No strategy returns computed. Exiting.");
                return;
            }

            // Step 3: Compute and print metrics (update MetricsCalculator to read CSV if it queries data)
            MetricsCalculator calculator = new MetricsCalculator(RISK_FREE_RATE, STRATEGY_START, STRATEGY_END);
            MetricsCalculator.MetricsReport report = calculator.compute(monthlyReturns, CSV_PATH);
            MetricsReporter.print(report);

            System.out.println("Backtest complete.");

        } catch (Exception e) {
            System.err.println("Error in backtest: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Checks if CSV exists and has sufficient data (row count POST-START > threshold AND date coverage).
     * Parses CSV to count post-start rows, find min/max dates.
     */
    private static boolean hasSufficientData(String csvPath) {
        File csvFile = new File(csvPath);
        if (!csvFile.exists()) {
            return false;
        }

        int totalRowCount = 0;
        int postStartRowCount = 0;
        LocalDate minDate = null;
        LocalDate maxDate = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (first) {
                    first = false;
                    continue;  // Skip header
                }
                String[] parts = line.split(",");
                if (parts.length < 3) continue;

                totalRowCount++;
                try {
                    LocalDate date = LocalDate.parse(parts[0], formatter);
                    if (minDate == null || date.isBefore(minDate)) minDate = date;
                    if (maxDate == null || date.isAfter(maxDate)) maxDate = date;
                    if (!date.isBefore(STRATEGY_START)) {
                        postStartRowCount++;
                    }
                } catch (Exception ignored) {
                    // Skip invalid rows
                }
            }
        } catch (IOException e) {
            System.err.println("CSV read error: " + e.getMessage() + ". Treating as insufficient.");
            return false;
        }

        if (postStartRowCount < MIN_ROWS_THRESHOLD) {
            System.out.println("CSV has only " + postStartRowCount + " post-" + STRATEGY_START + " rows (threshold: " + MIN_ROWS_THRESHOLD + ")—refetching.");
            return false;
        }

        if (minDate == null || maxDate == null) {
            return false;
        }

        boolean coversStart = !minDate.isAfter(STRATEGY_START);
        boolean coversEnd = !maxDate.isBefore(STRATEGY_END);

        if (!coversStart || !coversEnd) {
            System.out.println("CSV dates (" + minDate + " to " + maxDate + ") don't fully cover [" + STRATEGY_START + ", " + STRATEGY_END + "]—refetching.");
            return false;
        }

        System.out.println("CSV validated: " + totalRowCount + " total rows (" + postStartRowCount + " post-start), dates " + minDate + " to " + maxDate);
        return true;
    }
}