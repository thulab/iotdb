package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Not necessary. Use InvertExpressionVisitor
 */
public class Not implements Filter, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public Not(Filter that) {
        this.that = that;
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return !that.satisfy(digest);
    }

    @Override
    public boolean satisfy(TimeValuePair pair) {
        return !that.satisfy(pair);
    }

    @Override
    public boolean satisfy(long time, Object value) {
        return !that.satisfy(time, value);
    }


    public Filter getFilter() {
        return this.that;
    }

    @Override
    public String toString() {
        return "Not: " + that;
    }

}
