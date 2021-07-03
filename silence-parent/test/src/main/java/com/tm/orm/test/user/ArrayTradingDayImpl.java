package com.tm.orm.test.user;

import java.time.LocalDate;
import java.util.*;

/**
 * @Author yudm
 * @Date 2021/7/1 22:16
 * @Desc 利用数组可以通过索引下标迅速定位的特性，交易日越连续和饱和，速度越接近于treeSet，有时候甚至超过treeSet
 */
public class ArrayTradingDayImpl implements TradingDayService {
    //key是年，value是天的数组，天数代表数组下标，对应下标的值为1表示这一天为交易日
    private final Map<Integer, List<Byte>> model = new HashMap<>();
    //记录最大的年分
    private int maxYear = 0;

    public void addTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();
        List<Byte> days = model.get(year);
        //如果不存在
        if (null == days) {
            days = new ArrayList<>(356);
            for (int i = 0; i < 356; ++i) {
                days.add((byte) 0);
            }
            days.add(day - 1, (byte) 1);
            model.put(year, days);
        } else {
            days.add(day - 1, (byte) 1);
        }
        //记录最大年
        if (year > maxYear) {
            maxYear = year;
        }
    }

    @Override
    public boolean isTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();
        List<Byte> days = model.get(year);
        if (null == days) {
            return false;
        }
        //值为1表示是交易日
        return days.get(day - 1) == 1;
    }

    @Override
    public Optional<LocalDate> queryNextTradingDay(LocalDate date) {
        int year = date.getYear();
        int day = date.getDayOfYear();
        LocalDate d;
        //先遍历当年
        d = queryNext(year, day, 365);
        if (null != d) {
            return Optional.of(d);
        }
        //当年不存在则遍历往后的年
        for (int i = year + 1; i <= maxYear; ++i) {
            d = queryNext(i, 0, 365);
            if (null != d) {
                return Optional.of(d);
            }
        }
        //都不存在，返回空
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
        //如果开始日期和结束日期在同一年
        if (beginYear == endYear) {
            dates.addAll(iterateYear(beginYear, beginDay, endDay));
        } else {//如果开始和结束日期跨年了
            //先遍历当年
            dates.addAll(iterateYear(beginYear, beginDay, 365));
            //遍历往后的年
            endYear = Math.min(endYear, maxYear);
            for (int i = beginYear + 1; i <= endYear; ++i) {
                dates.addAll(iterateYear(i, 1, 365));
            }
            //遍历最后一年
            dates.addAll(iterateYear(endYear, 0, endDay));
        }
        return dates;
    }

    @Override
    public Optional<LocalDate> queryFirstTradingDayOfMonth(LocalDate date) {
        int year = date.getYear();
        int month = date.getMonthValue();
        //当月的第一天
        int beginDay = LocalDate.of(year, month, 1).getDayOfYear();
        //当月最后一天
        int endDay = LocalDate.of(year, month, date.lengthOfMonth()).getDayOfYear();
        return Optional.ofNullable(queryNext(year, beginDay - 1, endDay));
    }

    /**
     * @Param [year 年, beginDay 开始, endDay 结束]
     * @Desc 获取某年开始到结束的所有交易日
     */
    private List<LocalDate> iterateYear(int year, int beginDay, int endDay) {
        //获取year对应的天列表
        List<Byte> ds = model.get(year);
        List<LocalDate> dates = new ArrayList<>();
        if (null != ds) {
            for (int i = beginDay; i < endDay - 1; ++i) {
                //值为1表示是交易日
                if (ds.get(i) == 1) {
                    dates.add(LocalDate.ofYearDay(year, i + 1));
                }
            }
        }
        return dates;
    }

    /**
     * @Param [year 年, beginDay 开始, endDay 结束]
     * @Desc 获取某年开始到结束的第一个交易日
     */
    private LocalDate queryNext(int year, int beginDay, int endDay) {
        //获取year对应的天列表
        List<Byte> ds = model.get(year);
        if (null != ds) {
            for (int i = beginDay; i < endDay - 1; ++i) {
                //值为1表示是交易日
                if (ds.get(i) == 1) {
                    return LocalDate.ofYearDay(year, i + 1);
                }
            }
        }
        return null;
    }
}
