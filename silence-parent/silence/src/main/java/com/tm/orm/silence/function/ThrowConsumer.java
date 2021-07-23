package com.tm.orm.silence.function;

/**
 * @author yudm
 * @date 2021/7/20 15:19
 */
@FunctionalInterface
public interface ThrowConsumer<T> {
    void accept(T t) throws Exception;
}
