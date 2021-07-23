package com.tm.orm.silence.function;

/**
 * @author yudm
 * @date 2021/7/20 16:10
 */
@FunctionalInterface
public interface ThrowFunction<T, R> {
    R apply(T t) throws Exception;
}
