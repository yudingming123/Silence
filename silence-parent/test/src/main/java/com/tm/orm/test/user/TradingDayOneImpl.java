package com.tm.orm.test.user;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;

/**
 * @Author yudm
 * @Date 2021/7/1 23:21
 * @Desc
 */
public class TradingDayOneImpl implements TradingDayService {
    private static final TreeSet<Long> model = new TreeSet<>();

    public void addTradingDay(LocalDate date) {
        model.add(date.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli());
    }

    @Override
    public boolean isTradingDay(LocalDate date) {
        return model.contains(date.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli());
    }

    @Override
    public Optional<LocalDate> queryNextTradingDay(LocalDate date) {
        Long time = model.higher(date.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli());
        if (null == time) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(time));
    }

    @Override
    public List<LocalDate> queryBetween(LocalDate from, LocalDate to) {
        Long begin = model.higher(from.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli());
        Long end = model.higher(to.atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli());

        NavigableSet<Long> longs = model.subSet(begin, false, end, false);
        List<LocalDate> dates = new ArrayList<>();
        for (Long time : longs) {
            if (null == time) {
                continue;
            }
            dates.add(LocalDate.ofEpochDay(time));
        }
        return dates;
    }

    @Override
    public Optional<LocalDate> queryFirstTradingDayOfMonth(LocalDate date) {
        int year = date.getYear();
        int month = date.getMonthValue();
        long begin = LocalDate.of(year, month, 1).atStartOfDay().toInstant(ZoneOffset.of("+8")).toEpochMilli();
        Long time = model.higher(begin);
        if (null == time) {
            return Optional.empty();
        }
        LocalDate date1 = LocalDate.ofEpochDay(time);
        if (month != date1.getMonthValue()) {
            return Optional.empty();
        }
        return Optional.of(date1);
    }
}
