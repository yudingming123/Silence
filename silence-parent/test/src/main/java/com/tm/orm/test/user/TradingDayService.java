package com.tm.orm.test.user;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/**
 * @Author yudm
 * @Date 2021/7/1 22:15
 * @Desc
 */
public interface TradingDayService {
    //判断是否为交易日
    boolean isTradingDay(LocalDate date);

    //查询下一个交易日
    Optional<LocalDate> queryNextTradingDay(LocalDate date);

    //查询一段时间范围的交易日, 注意包括[from, to]
    List<LocalDate> queryBetween(LocalDate from, LocalDate to);

    //查询日期所属月份的第一个交易日
    Optional<LocalDate> queryFirstTradingDayOfMonth(LocalDate date);
}
