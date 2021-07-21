package com.tm.orm.silence.function;

/**
 * @Author yudm
 * @Date 2021/7/20 15:19
 */
@FunctionalInterface
public interface ThrowConsumer<T> {
    void accept(T t) throws Exception;
}
