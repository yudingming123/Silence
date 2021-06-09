package com.tm.silence.exception;


/**
 * @Author yudm
 * @Date 2020/12/25 16:05
 * @Desc 异常类
 */
public class SqlException extends RuntimeException {
    public SqlException() {
    }

    public SqlException(String msg) {
        super(msg);
    }

    public SqlException(Throwable t) {
        super(t);
    }

    public SqlException(String msg, Throwable t) {
        super(msg, t);
    }
}
