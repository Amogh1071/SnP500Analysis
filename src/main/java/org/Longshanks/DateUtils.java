// DateUtils.java
package org.Longshanks;

import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;

public class DateUtils {
    public static LocalDate getMonthEnd(LocalDate date) {
        return date.with(TemporalAdjusters.lastDayOfMonth());
    }
}

