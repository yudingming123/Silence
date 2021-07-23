package com.tm.orm.silence.function;

import java.sql.SQLException;

/**
 * @author yudm
 * @date 2021/7/20 14:52
 */
@FunctionalInterface
public interface BiThrowConsumer<P, V> {
    void accept(P p, V v) throws SQLException;
}
