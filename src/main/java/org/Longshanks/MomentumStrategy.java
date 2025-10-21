package org.Longshanks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
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
 * Dependencies: CSV read (assumes wide format from Python: Date,ticker1,ticker2,... with adj_close values).
 * Utils: getMonthEnd for monthly resampling, SeriesInterpolator for ffill/shift (stubbed).
 *
 * Usage: MomentumStrategy strategy = new MomentumStrategy(configParams);
 *        Map<LocalDate, Double> monthlyReturns = strategy.execute(csvPath);
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

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public MomentumStrategy(LocalDate startDate, LocalDate endDate) {
        this.START_DATE = startDate;
        this.END_DATE = endDate;
    }

    /**
     * Executes the strategy backtest.
     *
     * @param csvPath Path to CSV file with prices (wide format: Date,ticker1,ticker2,...).
     * @return Map<LocalDate, Double> of monthly strategy returns (date -> return).
     */
    public Map<LocalDate, Double> execute(String csvPath) {
        LOGGER.info("Executing Risk-Reduced Momentum Strategy...");

        // Step 1: Load and process data
        Map<LocalDate, Map<String, Double>> monthlyPrices = loadMonthlyPrices(csvPath);
        if (monthlyPrices.isEmpty()) {
            throw new IllegalStateException("No monthly price data loaded.");
        }

        // Step 2: Compute monthly returns (log)
        Map<LocalDate, Map<String, Double>> monthlyReturns = computeLogReturns(monthlyPrices);

        // Step 3: Compute indicators (EMA/SMA) and signals
        Map<LocalDate, Map<String, Double>> momSignals = computeMomentumSignals(monthlyPrices);

        // Step 4: Run strategy loop (quarterly rebalance)
        Map<LocalDate, Double> strategyReturnsMonthly = runBacktestLoop(monthlyReturns, momSignals, csvPath);

        LOGGER.info("Strategy execution complete. Monthly returns computed: " + strategyReturnsMonthly.size());
        return strategyReturnsMonthly;
    }

    private Map<LocalDate, Map<String, Double>> loadMonthlyPrices(String csvPath) {
        // Load tickers from header
        List<String> tickers = new ArrayList<>();
        Map<LocalDate, Map<String, Double>> dailyPrices = new TreeMap<>();

        int filteredRows = 0;
        boolean headerRead = false;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 2) continue;

                try {
                    LocalDate date = LocalDate.parse(parts[0], DATE_FORMATTER);
                    if (!headerRead) {
                        // Header: parts[0]="Date", parts[1..] = tickers
                        for (int i = 1; i < parts.length; i++) {
                            tickers.add(parts[i].trim());
                        }
                        headerRead = true;
                        LOGGER.info("Loaded " + tickers.size() + " tickers from header.");
                        continue;
                    }

                    if (date.isBefore(START_DATE) || date.isAfter(END_DATE)) continue;

                    Map<String, Double> dayPrices = new HashMap<>();
                    boolean hasValid = false;
                    for (int i = 1; i < parts.length; i++) {
                        if (i - 1 < tickers.size()) {
                            String ticker = tickers.get(i - 1);
                            double price = Double.parseDouble(parts[i]);
                            if (price > 0) {
                                dayPrices.put(ticker, price);
                                hasValid = true;
                            }
                        }
                    }
                    if (hasValid) {
                        dailyPrices.put(date, dayPrices);
                        filteredRows++;
                    }
                } catch (Exception e) {
                    // Skip invalid rows
                    LOGGER.fine("Skipped invalid row: " + line);
                }
            }
        } catch (IOException e) {
            LOGGER.severe("CSV load error: " + e.getMessage());
            return new HashMap<>();
        }

        LOGGER.info("Filtered " + filteredRows + " daily rows post-" + START_DATE + " with valid prices.");

        if (dailyPrices.isEmpty()) {
            LOGGER.warning("No daily data post-" + START_DATE + ". Check CSV content.");
            return new HashMap<>();
        }

        // Now resample to month-end: for each month, take prices from the last trading day
        Map<LocalDate, Map<String, Double>> monthlyPrices = new TreeMap<>();
        Map<String, Double> currentMonthPrices = new HashMap<>();
        LocalDate currentMonth = null;

        for (Map.Entry<LocalDate, Map<String, Double>> entry : dailyPrices.entrySet()) {
            LocalDate date = entry.getKey();
            LocalDate monthEnd = getMonthEnd(date);

            if (!monthEnd.equals(currentMonth)) {
                // Save previous month
                if (currentMonth != null && !currentMonthPrices.isEmpty()) {
                    monthlyPrices.put(currentMonth, new HashMap<>(currentMonthPrices));
                }
                // Start new month
                currentMonth = monthEnd;
                currentMonthPrices = new HashMap<>();
            }

            // Add/overwrite with this day's prices (later days overwrite earlier)
            Map<String, Double> dayPrices = entry.getValue();
            currentMonthPrices.putAll(dayPrices);
        }

        // Save last month
        if (currentMonth != null && !currentMonthPrices.isEmpty()) {
            monthlyPrices.put(currentMonth, new HashMap<>(currentMonthPrices));
        }

        // Clean: Drop tickers with <80% data (mimics Python dropna(thresh=0.8*len(prices)))
        monthlyPrices = cleanData(monthlyPrices);

        LOGGER.info("Loaded monthly prices for " + monthlyPrices.size() + " dates across " + getTotalTickers(monthlyPrices) + " tickers.");
        return monthlyPrices;
    }

    private int getTotalTickers(Map<LocalDate, Map<String, Double>> data) {
        Set<String> tickers = new HashSet<>();
        for (Map<String, Double> prices : data.values()) {
            tickers.addAll(prices.keySet());
        }
        return tickers.size();
    }

    private LocalDate getMonthEnd(LocalDate date) {
        return date.with(TemporalAdjusters.lastDayOfMonth());
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
                LOGGER.fine("Dropped ticker " + ticker + " (<80% coverage: " + count + "/" + data.size() + ")");
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
                                                   Map<LocalDate, Map<String, Double>> momSignals, String csvPath) {
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
            Map<String, Double> vols = computeDailyAnnualizedVol(csvPath, date, longStocks);
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

    private Map<String, Double> computeDailyAnnualizedVol(String csvPath, LocalDate endDate, List<String> tickers) {
        Map<String, Double> vols = new HashMap<>();
        LocalDate startVol = endDate.minusDays(300); // Buffer for 252 trading days

        // Load tickers from header (assume consistent)
        List<String> allTickers = new ArrayList<>();
        Map<String, TreeMap<LocalDate, Double>> tickerDailyPrices = new HashMap<>();
        Set<String> tickerSet = new HashSet<>(tickers);
        boolean headerRead = false;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 2) continue;

                try {
                    LocalDate date = LocalDate.parse(parts[0], DATE_FORMATTER);
                    if (!headerRead) {
                        for (int i = 1; i < parts.length; i++) {
                            allTickers.add(parts[i].trim());
                        }
                        headerRead = true;
                        continue;
                    }

                    if (date.isBefore(startVol) || date.isAfter(endDate)) continue;

                    for (int i = 1; i < parts.length; i++) {
                        if (i - 1 < allTickers.size()) {
                            String ticker = allTickers.get(i - 1);
                            if (tickerSet.contains(ticker)) {
                                double price = Double.parseDouble(parts[i]);
                                if (price > 0) {
                                    tickerDailyPrices.computeIfAbsent(ticker, k -> new TreeMap<>()).put(date, price);
                                }
                            }
                        }
                    }
                } catch (Exception ignored) {
                    // Skip
                }
            }
        } catch (IOException e) {
            LOGGER.warning("CSV vol calc error: " + e.getMessage() + ". Using default vols.");
            tickers.forEach(t -> vols.put(t, 0.2));
            return vols;
        }

        // Compute log returns per ticker using sorted dates
        for (String ticker : tickers) {
            TreeMap<LocalDate, Double> prices = tickerDailyPrices.get(ticker);
            if (prices == null || prices.size() < 2) {
                vols.put(ticker, 0.2); // Default
                continue;
            }

            List<Double> rets = new ArrayList<>();
            Iterator<Map.Entry<LocalDate, Double>> it = prices.entrySet().iterator();
            Map.Entry<LocalDate, Double> prevEntry = it.next();
            Double prevPrice = prevEntry.getValue();

            while (it.hasNext()) {
                Map.Entry<LocalDate, Double> entry = it.next();
                Double currPrice = entry.getValue();
                if (prevPrice > 0 && currPrice > 0) {
                    rets.add(Math.log(currPrice / prevPrice));
                }
                prevPrice = currPrice;
            }

            if (rets.size() < 30) { // Min for vol est.
                vols.put(ticker, 0.2);
                continue;
            }

            double sum = rets.stream().mapToDouble(Double::doubleValue).sum();
            double mean = sum / rets.size();
            double variance = rets.stream().mapToDouble(r -> Math.pow(r - mean, 2)).sum() / (rets.size() - 1);
            double std = Math.sqrt(variance);
            double annVol = std * Math.sqrt(252);
            vols.put(ticker, annVol > 0 ? annVol : 0.2);
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
    // Stub: Simple ffill (implement SeriesInterpolator if needed)
    public Map<LocalDate, Double> interpolateToDaily(Map<LocalDate, Double> monthlyReturns, LocalDate start, LocalDate end) {
        Map<LocalDate, Double> daily = new TreeMap<>();
        LocalDate lastDate = null;
        Double lastRet = 0.0; // Assume 0 for missing

        for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
            if (monthlyReturns.containsKey(date)) {
                lastRet = monthlyReturns.get(date);
                lastDate = date;
            }
            daily.put(date, lastRet);
        }
        return daily;
    }
}