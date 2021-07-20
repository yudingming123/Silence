package com.tm.orm.silence.function;


import java.sql.SQLException;

/**
 * @Author yudm
 * @Date 2021/7/20 14:52
 * @Desc
 */
@FunctionalInterface
public interface BiThrowConsumer<P, V> {
    void accept(P p, V v) throws SQLException;
}
