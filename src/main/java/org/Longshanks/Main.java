package org.Longshanks;

import org.Longshanks.MetricsCalculator;
import org.Longshanks.MetricsReporter;

import java.io.File;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Main entry point for the Risk-Reduced Momentum Backtest.
 *
 * Flow:
 * 1. Fetch data (if needed) via DataFetcher.
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

    public static void main(String[] args) {
        System.out.println("Starting Risk-Reduced Momentum Backtest...");

        try {

            // Step 1: Check if DB exists; fetch only if not
            File dbFile = new File(DB_PATH);
            if (!dbFile.exists()) {
                System.out.println("DB not found. Fetching data...");
                DataFetcher fetcher = new DataFetcher();
                fetcher.fetchAndSaveToDB(DB_PATH);
            } else {
                System.out.println("DB exists, skipping fetch.");
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
}