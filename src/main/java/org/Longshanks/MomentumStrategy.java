package org.Longshanks;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * MomentumStrategy implements the core backtesting logic for a risk-reduced EMA/SMA momentum strategy.
 *
 * Key Logic (mirroring Python):
 * - Loads monthly prices and computes log returns.
 * - Computes EMA (span=12) and SMA (span=50) on monthly prices.
 * - Momentum signal: EMA / SMA > UPTREND_THRESH (0.95).
 * - Quarterly rebalance: Select top QUINTILE (20%) of uptrend stocks (min MIN_STOCKS=10).
 * - Weights: Inverse vol (252-day ann.), cap at POS_CAP=0.05, renormalize.
 * - Returns: Weighted gross, apply STOP_LOSS_MONTHLY=-0.10, net after TX_COST=0.001 * TURNOVER_EST=0.25.
 * - Outputs monthly strategy returns, interpolated to daily.
 *
 * Dependencies: JDBC for DB read (assumes DataFetcher schema).
 * Utils: DateUtils for monthly resampling, SeriesInterpolator for ffill/shift.
 *
 * Usage: MomentumStrategy strategy = new MomentumStrategy(configParams);
 *        Map<LocalDate, Double> monthlyReturns = strategy.execute(dbPath);
 */
public class MomentumStrategy {

    private static final Logger LOGGER = Logger.getLogger(MomentumStrategy.class.getName());

    // Configurable params (inject via constructor or config loader in full app)
    private final int EMA_SPAN = 12;
    private final int SMA_SPAN = 50;
    private final double QUINTILE = 0.2;
    private final double UPTREND_THRESH = 0.95;
    private final int MIN_STOCKS = 10;
    private final double RISK_FREE_RATE = 0.02; // Not used here, for metrics
    private final double TX_COST = 0.001;
    private final double POS_CAP = 0.05;
    private final double STOP_LOSS_MONTHLY = -0.10;
    private final double TURNOVER_EST = 0.25;
    private final LocalDate START_DATE;
    private final LocalDate END_DATE;

    public MomentumStrategy(LocalDate startDate, LocalDate endDate) {
        this.START_DATE = startDate;
        this.END_DATE = endDate;
    }

    /**
     * Executes the strategy backtest.
     *
     * @param dbPath Path to SQLite DB with prices table.
     * @return Map<LocalDate, Double> of monthly strategy returns (date -> return).
     */
    public Map<LocalDate, Double> execute(String dbPath) {
        LOGGER.info("Executing Risk-Reduced Momentum Strategy...");

        // Step 1: Load and process data
        Map<LocalDate, Map<String, Double>> monthlyPrices = loadMonthlyPrices(dbPath);
        if (monthlyPrices.isEmpty()) {
            throw new IllegalStateException("No monthly price data loaded.");
        }

        // Step 2: Compute monthly returns (log)
        Map<LocalDate, Map<String, Double>> monthlyReturns = computeLogReturns(monthlyPrices);

        // Step 3: Compute indicators (EMA/SMA) and signals
        Map<LocalDate, Map<String, Double>> momSignals = computeMomentumSignals(monthlyPrices);

        // Step 4: Run strategy loop (quarterly rebalance)
        Map<LocalDate, Double> strategyReturnsMonthly = runBacktestLoop(monthlyReturns, momSignals, dbPath);

        LOGGER.info("Strategy execution complete. Monthly returns computed: " + strategyReturnsMonthly.size());
        return strategyReturnsMonthly;
    }

    private Map<LocalDate, Map<String, Double>> loadMonthlyPrices(String dbPath) {
        Map<LocalDate, Map<String, Double>> monthlyPrices = new TreeMap<>();

        String sql = "SELECT date, ticker, adj_close FROM prices " +
                "WHERE date >= ? AND date <= ? " +
                "ORDER BY date, ticker";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, START_DATE.toString());
            pstmt.setString(2, END_DATE.toString());

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    LocalDate date = LocalDate.parse(rs.getString("date"));
                    String ticker = rs.getString("ticker");
                    double price = rs.getDouble("adj_close");

                    // Resample to month-end: Use last price of each month
                    LocalDate monthEnd = DateUtils.getMonthEnd(date);
                    monthlyPrices.computeIfAbsent(monthEnd, k -> new HashMap<>()).put(ticker, price);
                }
            }

            // Clean: Drop tickers with <80% data
            monthlyPrices = cleanData(monthlyPrices);

        } catch (SQLException e) {
            LOGGER.severe("DB load error: " + e.getMessage());
            return new HashMap<>();
        }

        LOGGER.info("Loaded monthly prices for " + monthlyPrices.size() + " dates.");
        return monthlyPrices;
    }

    private Map<LocalDate, Map<String, Double>> cleanData(Map<LocalDate, Map<String, Double>> data) {
        // Drop tickers with NaN or insufficient data (threshold: 80% of dates)
        int minValid = (int) (data.size() * 0.8);
        Set<String> validTickers = new HashSet<>();

        for (Map.Entry<LocalDate, Map<String, Double>> entry : data.entrySet()) {
            entry.getValue().entrySet().removeIf(e -> Double.isNaN(e.getValue()) || e.getValue() <= 0);
            validTickers.addAll(entry.getValue().keySet());
        }

        Map<String, Integer> tickerCounts = new HashMap<>();
        for (String ticker : validTickers) {
            int count = 0;
            for (Map<String, Double> prices : data.values()) {
                if (prices.containsKey(ticker)) {
                    count++;
                }
            }
            tickerCounts.put(ticker, count);
            if (count < minValid) {
                for (Map<String, Double> prices : data.values()) {
                    prices.remove(ticker);
                }
            }
        }

        return data;
    }

    private Map<LocalDate, Map<String, Double>> computeLogReturns(Map<LocalDate, Map<String, Double>> prices) {
        Map<LocalDate, Map<String, Double>> returns = new TreeMap<>();
        Iterator<LocalDate> it = prices.keySet().iterator();
        if (!it.hasNext()) return returns;

        LocalDate prevDate = it.next();
        Map<String, Double> prevPrices = new HashMap<>(prices.get(prevDate));

        while (it.hasNext()) {
            LocalDate date = it.next();
            Map<String, Double> currPrices = prices.get(date);
            Map<String, Double> retMap = new HashMap<>();

            Set<String> allTickers = new HashSet<>(prevPrices.keySet());
            allTickers.addAll(currPrices.keySet());

            for (String ticker : allTickers) {
                Double currPrice = currPrices.get(ticker);
                Double prevPrice = prevPrices.get(ticker);
                if (currPrice != null && prevPrice != null && prevPrice > 0) {
                    retMap.put(ticker, Math.log(currPrice / prevPrice));
                }
            }
            returns.put(date, retMap);
            prevPrices = currPrices;
        }

        return returns;
    }

    private Map<LocalDate, Map<String, Double>> computeMomentumSignals(Map<LocalDate, Map<String, Double>> monthlyPrices) {
        Map<LocalDate, Map<String, Double>> signals = new TreeMap<>();

        List<LocalDate> dates = new ArrayList<>(monthlyPrices.keySet());
        int nDates = dates.size();
        if (nDates < SMA_SPAN) return signals;

        // For each ticker, compute EMA/SMA series
        Set<String> allTickers = monthlyPrices.values().stream()
                .flatMap(m -> m.keySet().stream()).collect(Collectors.toSet());

        for (String ticker : allTickers) {
            List<Double> pricesList = new ArrayList<>();
            for (LocalDate date : dates) {
                Double p = monthlyPrices.get(date).get(ticker);
                pricesList.add(p != null ? p : Double.NaN);
            }

            // Compute EMA and SMA
            List<Double> ema = computeEMA(pricesList, EMA_SPAN);
            List<Double> sma = computeSMA(pricesList, SMA_SPAN);

            // Shift for lag: signals from index SMA_SPAN onwards (lagged)
            for (int i = SMA_SPAN; i < nDates; i++) {
                Double emaVal = ema.get(i);
                Double smaVal = sma.get(i);
                if (!Double.isNaN(emaVal) && !Double.isNaN(smaVal) && smaVal > 0) {
                    double signal = emaVal / smaVal;
                    LocalDate date = dates.get(i);
                    signals.computeIfAbsent(date, k -> new HashMap<>()).put(ticker, signal);
                }
            }
        }

        return signals;
    }

    private List<Double> computeEMA(List<Double> values, int span) {
        int n = values.size();
        List<Double> ema = new ArrayList<>(n);
        if (n == 0) return ema;

        double alpha = 2.0 / (span + 1.0);
        Double prev = values.get(0);
        ema.add(prev != null ? prev : Double.NaN);

        for (int i = 1; i < n; i++) {
            Double val = values.get(i);
            if (Double.isNaN(val)) {
                ema.add(Double.NaN);
                prev = Double.NaN;
                continue;
            }
            if (Double.isNaN(prev)) {
                prev = val;
            } else {
                prev = alpha * val + (1 - alpha) * prev;
            }
            ema.add(prev);
        }
        return ema;
    }

    private List<Double> computeSMA(List<Double> values, int span) {
        int n = values.size();
        List<Double> sma = new ArrayList<>(n);
        if (n == 0) return sma;

        for (int i = 0; i < n; i++) {
            if (i < span - 1) {
                sma.add(Double.NaN);
                continue;
            }
            double sum = 0.0;
            int count = 0;
            for (int j = i - span + 1; j <= i; j++) {
                Double v = values.get(j);
                if (!Double.isNaN(v)) {
                    sum += v;
                    count++;
                }
            }
            sma.add(count > 0 ? sum / count : Double.NaN); // Use available count if NaNs
        }
        return sma;
    }

    private Map<LocalDate, Double> runBacktestLoop(Map<LocalDate, Map<String, Double>> monthlyReturns,
                                                   Map<LocalDate, Map<String, Double>> momSignals, String dbPath) {
        Map<LocalDate, Double> strategyReturns = new TreeMap<>();
        List<LocalDate> dates = new ArrayList<>(momSignals.keySet());

        // Quarterly: Every 3 months, starting after burn-in (SMA_SPAN months)
        int burnIn = SMA_SPAN;
        for (int i = burnIn; i < dates.size(); i += 3) { // Quarterly step
            LocalDate date = dates.get(i);
            if (!monthlyReturns.containsKey(date)) continue;

            // Get valid stocks at this date (lagged signal)
            Map<String, Double> signalsAtDate = momSignals.get(date);
            if (signalsAtDate == null || signalsAtDate.size() < 20) continue;

            // Filter uptrend stocks (> UPTREND_THRESH)
            List<Map.Entry<String, Double>> uptrendEntries = signalsAtDate.entrySet().stream()
                    .filter(e -> e.getValue() > UPTREND_THRESH)
                    .collect(Collectors.toList());

            if (uptrendEntries.size() < MIN_STOCKS) continue;

            // Rank and select top quintile of uptrend stocks
            uptrendEntries.sort(Map.Entry.<String, Double>comparingByValue().reversed());
            int nSelect = Math.max(MIN_STOCKS, (int) (uptrendEntries.size() * QUINTILE));
            List<String> longStocks = uptrendEntries.stream()
                    .limit(nSelect)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            if (longStocks.isEmpty()) continue;

            // Compute vol-scaled weights: Use daily returns for 252-day ann. vol (more accurate)
            Map<String, Double> vols = computeDailyAnnualizedVol(dbPath, date, longStocks);
            Map<String, Double> weights = computeInverseVolWeights(vols, POS_CAP);

            // Gross monthly return (weighted)
            double grossRet = 0.0;
            Map<String, Double> currentReturns = monthlyReturns.get(date);
            for (String ticker : longStocks) {
                Double ret = currentReturns.get(ticker);
                if (ret != null) {
                    grossRet += ret * weights.getOrDefault(ticker, 0.0);
                }
            }

            // Apply trailing stop-loss (-10% monthly max loss)
            grossRet = Math.max(grossRet, STOP_LOSS_MONTHLY);

            // Net after transaction costs
            double netRet = grossRet * (1 - TX_COST * TURNOVER_EST);
            strategyReturns.put(date, netRet);
        }

        return strategyReturns;
    }

    private Map<String, Double> computeDailyAnnualizedVol(String dbPath, LocalDate endDate, List<String> tickers) {
        Map<String, Double> vols = new HashMap<>();
        LocalDate startVol = endDate.minusDays(300); // Buffer for 252 trading days

        String sql = "SELECT date, ticker, adj_close FROM prices " +
                "WHERE date >= ? AND date <= ? AND ticker IN (" +
                String.join(",", Collections.nCopies(tickers.size(), "?")) + ") " +
                "ORDER BY ticker, date";

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, startVol.toString());
            pstmt.setString(2, endDate.toString());
            for (int i = 0; i < tickers.size(); i++) {
                pstmt.setString(3 + i, tickers.get(i));
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                Map<String, List<Double>> dailyLogRets = new HashMap<>();
                for (String ticker : tickers) {
                    dailyLogRets.put(ticker, new ArrayList<>());
                }

                String prevTicker = null;
                Double prevPrice = null;
                while (rs.next()) {
                    String ticker = rs.getString("ticker");
                    LocalDate date = LocalDate.parse(rs.getString("date"));
                    double price = rs.getDouble("adj_close");

                    if (!ticker.equals(prevTicker)) {
                        prevTicker = ticker;
                        prevPrice = null; // Reset for new ticker
                        continue;
                    }

                    if (prevPrice != null && prevPrice > 0) {
                        double logRet = Math.log(price / prevPrice);
                        dailyLogRets.get(ticker).add(logRet);
                    }
                    prevPrice = price;
                }

                // Compute std dev over ~252 days, annualize
                for (Map.Entry<String, List<Double>> entry : dailyLogRets.entrySet()) {
                    List<Double> rets = entry.getValue();
                    if (rets.size() < 30) { // Min for vol est.
                        vols.put(entry.getKey(), 0.2); // Default
                        continue;
                    }
                    double sum = rets.stream().mapToDouble(Double::doubleValue).sum();
                    double mean = sum / rets.size();
                    double variance = rets.stream().mapToDouble(r -> Math.pow(r - mean, 2)).sum() / (rets.size() - 1);
                    double std = Math.sqrt(variance);
                    double annVol = std * Math.sqrt(252);
                    vols.put(entry.getKey(), annVol > 0 ? annVol : 0.2);
                }
            }
        } catch (SQLException e) {
            LOGGER.warning("Vol calc error: " + e.getMessage() + ". Using default vols.");
            tickers.forEach(t -> vols.put(t, 0.2));
        }

        return vols;
    }

    private Map<String, Double> computeInverseVolWeights(Map<String, Double> vols, double posCap) {
        double sumInvVol = vols.values().stream().mapToDouble(v -> 1.0 / Math.max(v, 0.01)).sum(); // Avoid div0
        Map<String, Double> weights = new HashMap<>();

        for (Map.Entry<String, Double> entry : vols.entrySet()) {
            double invVol = 1.0 / Math.max(entry.getValue(), 0.01);
            double w = invVol / sumInvVol;
            w = Math.min(w, posCap);
            weights.put(entry.getKey(), w);
        }

        // Renormalize after capping
        double sumWeights = weights.values().stream().mapToDouble(Double::doubleValue).sum();
        if (sumWeights > 0) {
            weights.replaceAll((k, v) -> v / sumWeights);
        }

        return weights;
    }

    // Additional method to interpolate monthly to daily (called externally, e.g., in Metrics)
    public Map<LocalDate, Double> interpolateToDaily(Map<LocalDate, Double> monthlyReturns, LocalDate start, LocalDate end) {
        return SeriesInterpolator.ffillAndShift(monthlyReturns, start, end);
    }
}