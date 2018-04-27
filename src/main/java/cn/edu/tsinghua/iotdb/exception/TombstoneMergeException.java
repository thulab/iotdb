package cn.edu.tsinghua.iotdb.exception;

public class TombstoneMergeException extends FileNodeProcessorException {
    public TombstoneMergeException() {
    }

    public TombstoneMergeException(PathErrorException pathExcp) {
        super(pathExcp);
    }

    public TombstoneMergeException(String msg) {
        super(msg);
    }

    public TombstoneMergeException(Throwable throwable) {
        super(throwable);
    }
}
