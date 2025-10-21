package org.Longshanks;

import org.Longshanks.MomentumStrategy;
import org.Longshanks.SeriesInterpolator;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * MetricsCalculator computes performance metrics for the strategy and market benchmark.
 *
 * Metrics (annualized, daily freq=252):
 * - Ann Return: mean(returns) * 252
 * - Ann Vol: std(returns) * sqrt(252)
 * - Sharpe: (Ann Return - RF) / Ann Vol
 * - Max DD: min( (cumprod / cummax - 1) )
 * - Sortino: (Ann Return - RF) / downside std * sqrt(252)
 * - Calmar: Ann Return / |Max DD|
 *
 * Benchmark: Equal-weight market (mean daily log returns across stocks).
 *
 * Usage: MetricsCalculator calc = new MetricsCalculator(rf, start, end);
 *        MetricsReport report = calc.compute(strategyMonthlyReturns, dbPath);
 *        MetricsReporter.print(report);
 */
public class MetricsCalculator {

    private static final Logger LOGGER = Logger.getLogger(MetricsCalculator.class.getName());
    private static final int FREQ = 252; // Trading days/year

    private final double riskFreeRate;
    private final LocalDate startDate;
    private final LocalDate endDate;

    public MetricsCalculator(double rf, LocalDate start, LocalDate end) {
        this.riskFreeRate = rf;
        this.startDate = start;
        this.endDate = end;
    }

    /**
     * Computes metrics for strategy and benchmark.
     *
     * @param strategyMonthlyReturns Monthly strategy returns from MomentumStrategy.
     * @param dbPath Path to prices DB.
     * @return MetricsReport with strategy and benchmark metrics.
     */
    public MetricsReport compute(Map<LocalDate, Double> strategyMonthlyReturns, String dbPath) {
        LOGGER.info("Computing performance metrics...");

        // Interpolate strategy to daily
        Map<LocalDate, Double> strategyDailyReturns = interpolateStrategyReturns(strategyMonthlyReturns);

        // Compute benchmark daily returns (equal-weight)
        Map<LocalDate, Double> benchmarkDailyReturns = computeBenchmarkReturns(dbPath);

        // Align dates (intersection)
        Set<LocalDate> commonDates = new HashSet<>(strategyDailyReturns.keySet());
        commonDates.retainAll(benchmarkDailyReturns.keySet());
        List<LocalDate> alignedDates = commonDates.stream().sorted().collect(Collectors.toList());

        List<Double> stratRets = alignedDates.stream()
                .map(d -> strategyDailyReturns.getOrDefault(d, 0.0))
                .collect(Collectors.toList());
        List<Double> benchRets = alignedDates.stream()
                .map(d -> benchmarkDailyReturns.getOrDefault(d, 0.0))
                .collect(Collectors.toList());

        if (stratRets.isEmpty()) {
            throw new IllegalStateException("No aligned daily returns for metrics.");
        }

        // Compute metrics
        Map<String, Double> stratMetrics = computeMetrics(stratRets);
        Map<String, Double> benchMetrics = computeMetrics(benchRets);

        return new MetricsReport(stratMetrics, benchMetrics);
    }

    private Map<LocalDate, Double> interpolateStrategyReturns(Map<LocalDate, Double> monthlyReturns) {
        // Use shared interpolator (assumes MomentumStrategy has it public/static or copy logic)
        return SeriesInterpolator.ffillAndShift(monthlyReturns, startDate, endDate);
    }

    private Map<LocalDate, Double> computeBenchmarkReturns(String dbPath) {
        Map<LocalDate, Double> dailyReturns = new TreeMap<>();

        String sql = "SELECT date, ticker, adj_close FROM prices " +
                "WHERE date >= ? AND date <= ? " +
                "ORDER BY date, ticker";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, startDate.toString());
            pstmt.setString(2, endDate.toString());

            try (ResultSet rs = pstmt.executeQuery()) {
                Map<LocalDate, Map<String, Double>> dailyPrices = new TreeMap<>();

                while (rs.next()) {
                    LocalDate date = LocalDate.parse(rs.getString("date"));
                    String ticker = rs.getString("ticker");
                    double price = rs.getDouble("adj_close");

                    dailyPrices.computeIfAbsent(date, k -> new HashMap<>()).put(ticker, price);
                }

                // Compute daily log returns per stock, then equal-weight mean
                Iterator<LocalDate> it = dailyPrices.keySet().iterator();
                if (!it.hasNext()) return dailyReturns;

                LocalDate prevDate = it.next();
                Map<String, Double> prevPrices = dailyPrices.get(prevDate);

                while (it.hasNext()) {
                    LocalDate date = it.next();
                    Map<String, Double> currPrices = dailyPrices.get(date);

                    double totalLogRet = 0.0;
                    int validStocks = 0;

                    Set<String> allTickers = new HashSet<>(prevPrices.keySet());
                    allTickers.retainAll(currPrices.keySet());

                    for (String ticker : allTickers) {
                        Double currPrice = currPrices.get(ticker);
                        Double prevPrice = prevPrices.get(ticker);
                        if (currPrice != null && prevPrice != null && prevPrice > 0) {
                            totalLogRet += Math.log(currPrice / prevPrice);
                            validStocks++;
                        }
                    }

                    if (validStocks > 0) {
                        dailyReturns.put(date, totalLogRet / validStocks);
                    }

                    prevPrices = currPrices;
                }
            }

        } catch (SQLException e) {
            LOGGER.severe("Benchmark computation error: " + e.getMessage());
        }

        LOGGER.info("Benchmark daily returns: " + dailyReturns.size() + " dates.");
        return dailyReturns;
    }

    private Map<String, Double> computeMetrics(List<Double> returns) {
        if (returns.isEmpty()) {
            return defaultMetrics();
        }

        // Ann Return and Vol
        double meanDaily = returns.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double annReturn = meanDaily * FREQ;
        double stdDaily = stdDev(returns);
        double annVol = stdDaily * Math.sqrt(FREQ);

        double sharpe = annVol > 0 ? (annReturn - riskFreeRate) / annVol : 0.0;

        // Cumulative returns for DD
        List<Double> cumRets = new ArrayList<>();
        double cum = 1.0;
        for (double r : returns) {
            cum *= (1 + r);
            cumRets.add(cum - 1); // Simple return for DD
        }

        double maxCum = cumRets.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
        double maxDd = 0.0;
        double peak = 0.0;
        for (double cr : cumRets) {
            if (cr > peak) peak = cr;
            double dd = (cr - peak) / (peak + 1); // Normalized DD
            if (dd < maxDd) maxDd = dd;
        }

        // Sortino
        List<Double> downside = returns.stream().filter(r -> r < 0).collect(Collectors.toList());
        double downsideStd = downside.isEmpty() ? 0.0 : stdDev(downside);
        double annDownside = downsideStd * Math.sqrt(FREQ);
        double sortino = annDownside > 0 ? (annReturn - riskFreeRate) / annDownside : 0.0;

        // Calmar
        double calmar = maxDd != 0 ? annReturn / Math.abs(maxDd) : 0.0;

        Map<String, Double> metrics = new HashMap<>();
        metrics.put("Ann Return", annReturn);
        metrics.put("Ann Vol", annVol);
        metrics.put("Sharpe", sharpe);
        metrics.put("Max DD", maxDd);
        metrics.put("Sortino", sortino);
        metrics.put("Calmar", calmar);

        return metrics;
    }

    private double stdDev(List<Double> values) {
        if (values.size() <= 1) return 0.0;
        double mean = values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = values.stream().mapToDouble(v -> Math.pow(v - mean, 2)).sum() / (values.size() - 1);
        return Math.sqrt(variance);
    }

    private Map<String, Double> defaultMetrics() {
        Map<String, Double> metrics = new HashMap<>();
        metrics.put("Ann Return", 0.0);
        metrics.put("Ann Vol", 0.0);
        metrics.put("Sharpe", 0.0);
        metrics.put("Max DD", 0.0);
        metrics.put("Sortino", 0.0);
        metrics.put("Calmar", 0.0);
        return metrics;
    }

    /**
     * Simple POJO for metrics report.
     */
    public static class MetricsReport {
        public final Map<String, Double> strategyMetrics;
        public final Map<String, Double> benchmarkMetrics;

        public MetricsReport(Map<String, Double> strategy, Map<String, Double> benchmark) {
            this.strategyMetrics = strategy;
            this.benchmarkMetrics = benchmark;
        }
    }
}

/**
 * MetricsReporter formats and prints the report (console table).
 */
class MetricsReporter {

    private static final Logger LOGGER = Logger.getLogger(MetricsReporter.class.getName());

    /**
     * Prints metrics as a simple table.
     */
    public static void print(MetricsCalculator.MetricsReport report) {
        LOGGER.info("Performance Metrics:");
        System.out.println("\nMetric          | Risk-Reduced Momentum | Market Benchmark");
        System.out.println("----------------|--------------------|------------------");

        List<String> metricKeys = Arrays.asList("Ann Return", "Ann Vol", "Sharpe", "Max DD", "Sortino", "Calmar");
        for (String key : metricKeys) {
            double stratVal = report.strategyMetrics.getOrDefault(key, 0.0);
            double benchVal = report.benchmarkMetrics.getOrDefault(key, 0.0);
            System.out.printf("%-15s | %10.4f        | %10.4f%n", key, stratVal, benchVal);
        }
        System.out.println();
    }
}