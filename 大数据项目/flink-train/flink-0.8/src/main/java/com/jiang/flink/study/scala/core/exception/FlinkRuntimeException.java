package com.jiang.flink.study.scala.core.exception;

/**
 * @ClassName FlinkRuntimeException
 * @Description TODO
 * @Author jiang.li
 * @Date 2019-12-07 19:03
 * @Version 1.0
 */
public class FlinkRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 193141189399279147L;

    public FlinkRuntimeException(){ super();}


    /**
     * Creates a new Exception with the given message and null as the cause.
     *
     * @param message The exception message
     */
    public FlinkRuntimeException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with a null message and the given cause.
     *
     * @param cause The exception that caused this exception
     */
    public FlinkRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message
     * @param cause The exception that caused this exception
     */
    public FlinkRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
