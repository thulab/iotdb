package cn.edu.tsinghua.iotdb.index.common;

import cn.edu.tsinghua.iotdb.qp.exception.QueryProcessorException;

public class IndexManagerException extends QueryProcessorException {

    private static final long serialVersionUID = 6261687971768311032L;

    public IndexManagerException(String message) {
        super(message);
    }

    public IndexManagerException(Throwable cause) {
        super(cause);
    }
}
