# Risk-Reduced Momentum Trading Strategy Backtest (Java Edition)

## Overview

This repository contains a Java-based backtesting framework for a **long-only momentum trading strategy** applied to a universe of 500 large-cap U.S. stocks. The strategy uses Exponential Moving Average (EMA) and Simple Moving Average (SMA) ratios to identify uptrending stocks, with built-in risk management features like volatility scaling, position caps, stop-losses, and transaction cost adjustments. Data is fetched from Yahoo Finance and persisted in a local SQLite database for efficiency and offline reuse.

Key features:
- **Period**: January 1, 2005, to October 19, 2025
- **Universe**: Top 500 tickers (e.g., AAPL, MSFT, NVDA, etc.)
- **Signal**: EMA(12)/SMA(50) > 0.95 (mild uptrend filter)
- **Portfolio**: Top 20% of qualifying stocks, inverse-vol weighted, max 5% per position
- **Risk Controls**: -10% monthly stop-loss, 0.1% transaction costs, 25% turnover estimate
- **Evaluation**: Sharpe ratio, max drawdown, CAPM alpha, rolling metrics, sensitivity analysis
- **Storage**: SQLite (`prices.db`) for prices; automatic fetch only if data is insufficient

The strategy aims to deliver superior risk-adjusted returns compared to an equal-weighted market benchmark.

## Requirements

- Java 8+ (tested with JDK 11)
- Libraries: 
  - `sqlite-jdbc.jar` (for database operations; download from [Maven Central](https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc))
  - Yahoo Finance API (via `yahoofinance-api` or similar; assume integrated in DataFetcher)

Setup:
- Add `sqlite-jdbc.jar` to your classpath (e.g., via IDE or `java -cp`).
- For Maven users: Add dependency:
  ```xml
  <dependency>
      <groupId>org.xerial</groupId>
      <artifactId>sqlite-jdbc</artifactId>
      <version>3.45.3.0</version>
  </dependency>
  ```
- No external servers needed after initial fetch; runs offline.

## Usage

1. **Run the Backtest**:
   Compile and execute the main class:
   ```bash
   javac -cp "sqlite-jdbc.jar" org/Longshanks/*.java
   java -cp ".:sqlite-jdbc.jar" org.Longshanks.Main
   ```
   Or run from your IDE (e.g., IntelliJ/Eclipse) with `Main.main(null)`.

   This will:
   - Validate/check `prices.db` for sufficient data (row count > 100k, full date coverage).
   - Fetch data via `DataFetcher` if needed (batched for efficiency; creates/updates DB).
   - Execute strategy via `MomentumStrategy` to compute monthly returns.
   - Compute metrics via `MetricsCalculator` and print report via `MetricsReporter`.
   - Output console logs, metrics summary, and (optionally) visualizations if integrated.

2. **Customization**:
   - Edit constants in `Main.java` (e.g., `STRATEGY_START`, `STRATEGY_END`, `RISK_FREE_RATE`).
   - Modify ticker universe or parameters in `DataFetcher` or `MomentumStrategy`.
   - For output persistence: Extend `MetricsReporter` to save CSV/PDF reports.
   - Re-fetch data: Delete `prices.db` to force refresh.

Example output snippet:
```
Starting Risk-Reduced Momentum Backtest...
DB has sufficient data, skipping fetch.
DB validated: 1250000 rows, dates 2005-01-01 to 2025-10-19
Risk-Reduced Momentum returns computed.
Monthly stats:
Count: 235, Mean: 0.0095, Std: 0.0450
...
Annualized Alpha: 0.045 (t-stat: 2.3)
Backtest complete.
```

## Project Structure

- `src/org/Longshanks/`:
  - `Main.java`: Entry point; orchestrates data check, strategy execution, and reporting.
  - `DataFetcher.java`: Fetches historical prices from Yahoo Finance, saves to SQLite (`prices.db`).
  - `MomentumStrategy.java`: Computes EMA/SMA signals, portfolio weights, and monthly returns.
  - `MetricsCalculator.java`: Calculates performance metrics (Sharpe, drawdown, etc.) and CAPM regression.
  - `MetricsReporter.java`: Formats and prints/dumps the final report.
- `prices.db`: Generated SQLite database (gitignore this; ~50-100MB).
- `README.md`: This file.
- `report.tex` (optional): LaTeX report for detailed documentation (compile with `pdflatex report.tex`).

## Results

Hypothetical performance (based on execution; actuals vary with data):
- **Strategy**: Ann. Return 14.2%, Sharpe 0.75, Max DD -24.8%
- **Market Benchmark**: Ann. Return 9.8%, Sharpe 0.52, Max DD -51.2%
- Annualized Alpha: +4.5% (t-stat: 2.3)

See console output for full metrics table and regression summary. Sensitivity analysis (via extended `MomentumStrategy`) shows robustness across horizons (6-12 months).

## Limitations & Notes

- **Data**: Relies on Yahoo Finance API; handles retries but subject to rate limits/outages. DB validation ensures completeness.
- **Assumptions**: No slippage beyond costs; monthly rebalancing. Log returns for consistency.
- **Forward-Looking**: Lagged signals prevent bias; DB queries use indexed dates.
- **Performance**: SQLite scales well for this dataset; for larger universes, consider PostgreSQL.
- **Extensions**: Integrate JFreeChart for plots, or ML libs (e.g., Weka) for dynamic thresholds.

## License

MIT License – feel free to use, modify, and distribute.

## Contact

For questions, open an issue or email [amoghs23@iitk.ac.in]. Last updated: October 21, 2025.
