package cn.edu.tsinghua.iotdb.exception;

import java.io.IOException;

public class WALOverSizedException extends IOException {
    public WALOverSizedException() {
        super();
    }

    public WALOverSizedException(String message) {
        super(message);
    }

    public WALOverSizedException(Throwable cause) {
        super(cause);
    }
}
