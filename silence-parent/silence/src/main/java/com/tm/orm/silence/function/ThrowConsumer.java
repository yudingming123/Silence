package com.tm.orm.silence.function;

import java.sql.SQLException;

/**
 * @Author yudm
 * @Date 2021/7/20 15:19
 * @Desc
 */
@FunctionalInterface
public interface ThrowConsumer<T> {
    void accept(T t) throws Exception;
}
