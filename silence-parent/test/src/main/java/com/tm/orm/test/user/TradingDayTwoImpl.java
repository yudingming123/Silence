package com.tm.orm.test.user;

import java.time.LocalDate;
import java.util.*;

/**
 * @Author yudm
 * @Date 2021/7/1 22:16
 * @Desc
 */
public class TradingDayTwoImpl implements TradingDayService {
    private final Map<Integer, List<Byte>> model = new HashMap<>();

    public void addTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();
        if (model.containsKey(year)) {
            model.get(year).add(day, (byte) 1);
        } else {
            List<Byte> days = new ArrayList<>(356);
            days.add(day - 1, (byte) 1);
            model.put(year, days);
        }
    }

    @Override
    public boolean isTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();
        if (!model.containsKey(year)) {
            return false;
        }
        return model.get(year).get(day - 1) == 1;
    }

    @Override
    public Optional<LocalDate> queryNextTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();

        LocalDate now = LocalDate.now();
        int lastYear = now.getYear();
        int lastDay = now.getDayOfYear();

        if (model.containsKey(year)) {
            for (int i = day; i < lastDay; ++i) {
                if (model.get(year).get(i) == 1) {
                    return Optional.of(LocalDate.ofYearDay(year, i));
                }
            }
        }
        for (int i = year + 1; i < lastYear; ++i) {
            if (!model.containsKey(i)) {
                continue;
            }
            for (int j = 0; j < 365; ++j) {
                if (model.get(i).get(j) == 1) {
                    return Optional.of(LocalDate.ofYearDay(i, j));
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public List<LocalDate> queryBetween(LocalDate from, LocalDate to) {
        if (from.compareTo(to) >= 0) {
            throw new RuntimeException("'from' can not bigger than 'to'");
        }
        int beginYear = from.getYear();
        int beginDay = from.getDayOfYear();
        int endYear = to.getYear();
        int endDay = to.getDayOfYear();
        List<LocalDate> dates = new ArrayList<>();
        if (beginYear == endYear) {
            List<Byte> ds = model.get(beginYear);
            for (int i = beginDay; i < endDay; ++i) {
                if (ds.get(i) == 1) {
                    dates.add(LocalDate.ofYearDay(beginYear, i));
                }
            }
        } else {
            if (model.containsKey(beginYear)) {
                for (int i = beginDay; i < 356; ++i) {
                    List<Byte> ds = model.get(beginYear);
                    if (ds.get(i) == 1) {
                        dates.add(LocalDate.ofYearDay(beginYear, i));
                    }
                }
            }
            for (int i = beginYear + 1; i < endYear; ++i) {
                if (!model.containsKey(i)) {
                    continue;
                }
                for (int j = 0; j < endDay; ++j) {
                    if (model.get(i).get(j) == 1) {
                        dates.add(LocalDate.ofYearDay(i, j));
                    }
                }
            }
        }
        return dates;
    }

    @Override
    public Optional<LocalDate> queryFirstTradingDayOfMonth(LocalDate date) {
        int year = date.getYear();
        int month = date.getMonthValue();
        int beginDay = LocalDate.of(year, month, 1).getDayOfYear();
        int endDay = LocalDate.of(year, month, date.lengthOfMonth()).getDayOfYear();
        if (model.containsKey(year)) {
            List<Byte> ds = model.get(year);
            for (int i = beginDay; i < endDay; ++i) {
                if (ds.get(i) == 1) {
                    return Optional.of(LocalDate.ofYearDay(year, i));
                }
            }
        }
        return Optional.empty();
    }
}
