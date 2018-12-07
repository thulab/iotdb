package cn.edu.tsinghua.tsfile.exception;

/**
 * This Exception is the parent class for all runtime exceptions.<br>
 * This Exception extends super class {@link java.lang.RuntimeException}
 *
 * @author kangrong
 */
abstract public class TsFileRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 6455048223316780984L;

    public TsFileRuntimeException() {
        super();
    }

    public TsFileRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public TsFileRuntimeException(String message) {
        super(message);
    }

    public TsFileRuntimeException(Throwable cause) {
        super(cause);
    }
}
