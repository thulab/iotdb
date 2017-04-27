package cn.edu.thu.tsfile.spark.common;


import cn.edu.thu.tsfile.spark.exception.BasicOperatorException;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;

/**
 * basic operators include < > >= <= !=.
 * 
 * @author kangrong
 *
 */

public class BasicOperator extends FilterOperator {

    private String seriesPath;
    private String seriesValue;

    public String getSeriesPath() {
        return seriesPath;
    }

    public String getSeriesValue() {
        return seriesValue;
    }

    public BasicOperator(int tokenIntType, String path, String value) {
        super(tokenIntType);
        this.seriesPath = this.singlePath = path;
        this.seriesValue = value;
        this.isLeaf = true;
        this.isSingle = true;
        if(path.equals(SQLConstant.RESERVED_DELTA_OBJECT) || path.equals(SQLConstant.RESERVED_TIME)) {
            hasReserve = true;
        }
    }

    public void setReversedTokenIntType() throws BasicOperatorException {
        int intType = SQLConstant.reverseWords.get(tokenIntType);
        setTokenIntType(intType);
    }

    @Override
    public String getSinglePath() {
        return singlePath;
    }


    @Override
    public BasicOperator clone() {
        BasicOperator ret;
        ret = new BasicOperator(this.tokenIntType, seriesPath, seriesValue);
        ret.tokenSymbol=tokenSymbol;
        ret.isLeaf = isLeaf;
        ret.isSingle = isSingle;
        return ret;
    }
    
    @Override
    public String toString() {
        return "[" + seriesPath + tokenSymbol + seriesValue + "]";
    }
}
