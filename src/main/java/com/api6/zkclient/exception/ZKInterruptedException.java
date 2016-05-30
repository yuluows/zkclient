package com.api6.zkclient.exception;

public class ZKInterruptedException extends ZKException {

    private static final long serialVersionUID = 1L;

    public ZKInterruptedException(InterruptedException e) {
        super(e);
        Thread.currentThread().interrupt();
    }
}
