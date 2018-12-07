package cn.edu.tsinghua.tsfile.timeseries.filter.exception;


public class QueryFilterOptimizationException extends Exception{

    public QueryFilterOptimizationException(String msg){
        super(msg);
    }

    public QueryFilterOptimizationException(Throwable cause){
        super(cause);
    }

    public QueryFilterOptimizationException(String message, Throwable cause) {
        super(message, cause);
    }
}
