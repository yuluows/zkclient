package com.api6.zkclient.exception;

import org.apache.zookeeper.KeeperException;

public class ZKException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ZKException() {
        super();
    }

    public ZKException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZKException(String message) {
        super(message);
    }

    public ZKException(Throwable cause) {
        super(cause);
    }

    public static ZKException create(KeeperException e) {
        switch (e.code()) {
        case NONODE:
            return new ZKNoNodeException(e);
        case NODEEXISTS:
            return new ZKNodeExistsException(e);
        default:
            return new ZKException(e);
        }
    }
}
