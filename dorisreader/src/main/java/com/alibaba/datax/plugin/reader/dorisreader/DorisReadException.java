package com.alibaba.datax.plugin.reader.dorisreader;


import org.apache.doris.sdk.thrift.TStatusCode;

import java.util.List;

public class DorisReadException extends RuntimeException {

    public DorisReadException() {
        super();
    }

    public DorisReadException(String message) {
        super(message);
    }

    public DorisReadException(String server, Throwable cause) {
        super("Connect to " + server + " failed.", cause);
    }

    public DorisReadException(String server, int statusCode, Throwable cause) {
        super("Connect to " + server + " failed, status code is " + statusCode + ".", cause);
    }


    public DorisReadException(String server, TStatusCode statusCode, List<String> errorMsgs) {
        super(
                "Doris server "
                        + server
                        + " internal failed, status code ["
                        + statusCode
                        + "] error message is "
                        + errorMsgs);
    }


    public DorisReadException(Throwable cause) {
        super(cause);
    }
}
