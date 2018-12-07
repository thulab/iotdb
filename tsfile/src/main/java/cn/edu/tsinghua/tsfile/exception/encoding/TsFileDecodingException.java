package cn.edu.tsinghua.tsfile.exception.encoding;

import cn.edu.tsinghua.tsfile.exception.TsFileRuntimeException;

/**
 * This Exception is used while decoding failed. <br>
 * This Exception extends super class
 * {@link TsFileRuntimeException}
 *
 * @author kangrong
 */
public class TsFileDecodingException extends TsFileRuntimeException {
    private static final long serialVersionUID = -8632392900655017028L;

    public TsFileDecodingException() {
    }

    public TsFileDecodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TsFileDecodingException(String message) {
        super(message);
    }

    public TsFileDecodingException(Throwable cause) {
        super(cause);
    }
}
