package cn.edu.tsinghua.tsfile.exception.encoding;

import cn.edu.tsinghua.tsfile.exception.TsFileRuntimeException;

/**
 * This Exception is used while encoding failed. <br>
 * This Exception extends super class
 * {@link TsFileRuntimeException}
 *
 * @author kangrong
 */
public class TsFileEncodingException extends TsFileRuntimeException {
    private static final long serialVersionUID = -7225811149696714845L;

    public TsFileEncodingException() {
    }

    public TsFileEncodingException(String message, Throwable cause) {
        super(message, cause);
    }

    public TsFileEncodingException(String message) {
        super(message);
    }

    public TsFileEncodingException(Throwable cause) {
        super(cause);
    }
}
