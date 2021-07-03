package com.tm.orm.test.user;

import java.time.LocalDate;
import java.util.*;

/**
 * @Author yudm
 * @Date 2021/7/1 23:21
 * @Desc 通过TreeSet来实现，底层是红黑树，插入时会自动排序，查询速度快
 */
public class TreeSetTradingDayImpl implements TradingDayService {
    private static final TreeSet<Long> model = new TreeSet<>();

    @Override
    public synchronized void addTradingDay(LocalDate date) {
        //转化成long，节约空间，提升比较和查询速度
        model.add(date.toEpochDay());
    }

    @Override
    public synchronized boolean isTradingDay(LocalDate date) {
        return model.contains(date.toEpochDay());
    }

    @Override
    public Optional<LocalDate> queryNextTradingDay(LocalDate date) {
        Long time;
        synchronized (this) {
            time = model.higher(date.toEpochDay());
        }
        if (null == time) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(time));
    }

    @Override
    public List<LocalDate> queryBetween(LocalDate from, LocalDate to) {
        List<LocalDate> dates = new ArrayList<>();
        synchronized (this) {
            model.subSet(from.toEpochDay(), false, to.toEpochDay(), false).forEach(m -> {
                if (null != m) {
                    dates.add(LocalDate.ofEpochDay(m));
                }
            });
        }
        return dates;
    }

    @Override
    public Optional<LocalDate> queryFirstTradingDayOfMonth(LocalDate date) {
        //获取当月的第一天
        int year = date.getYear();
        int month = date.getMonthValue();
        long begin = LocalDate.of(year, month, 1).toEpochDay();
        //获取下一个
        Long time;
        synchronized (this) {
            time = model.ceiling(begin);
        }
        if (null == time) {
            return Optional.empty();
        }
        //判断是否是同一个月
        LocalDate date1 = LocalDate.ofEpochDay(time);
        if (month != date1.getMonthValue()) {
            return Optional.empty();
        }
        return Optional.of(date1);
    }
}
