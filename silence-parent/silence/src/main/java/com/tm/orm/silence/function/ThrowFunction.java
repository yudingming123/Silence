package com.tm.orm.silence.function;

/**
 * @Author yudm
 * @Date 2021/7/20 16:10
 * @Desc
 */
@FunctionalInterface
public interface ThrowFunction<T> {
    <R> R apply(T t) throws Exception;
}
