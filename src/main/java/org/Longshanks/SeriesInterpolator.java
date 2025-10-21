// SeriesInterpolator.java
package org.Longshanks;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SeriesInterpolator {
    public static Map<LocalDate, Double> ffillAndShift(Map<LocalDate, Double> monthly, LocalDate start, LocalDate end) {
        Map<LocalDate, Double> daily = new TreeMap<>();
        Double lastVal = null;
        LocalDate prevKey = null;

        // Ffill monthly to daily grid
        for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
            Double val = monthly.get(d.withDayOfMonth(1).minusDays(1)); // Nearest month-end before/equal
            if (val != null) {
                lastVal = val;
                prevKey = d;
            }
            if (lastVal != null) {
                daily.put(d, lastVal);
            }
        }

        // Shift forward by 1 day (signal lag)
        Map<LocalDate, Double> shifted = new HashMap<>();
        for (Map.Entry<LocalDate, Double> e : daily.entrySet()) {
            LocalDate shiftedDate = e.getKey().plusDays(1);
            if (!shiftedDate.isAfter(end)) {
                shifted.put(shiftedDate, e.getValue());
            }
        }
        return shifted;
    }
}