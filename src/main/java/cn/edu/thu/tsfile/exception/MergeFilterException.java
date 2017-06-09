package cn.edu.thu.tsfile.exception;


/**
 * This exception is threw whiling meeting error in
 *
 */
public class MergeFilterException extends LogicalOptimizeException {

    private static final long serialVersionUID = 8581594261924961899L;

    public MergeFilterException(String msg) {
        super(msg);
    }

}
