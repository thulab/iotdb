package cn.edu.tsinghua.tsfile.read.filter.operator;

import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;

import java.io.Serializable;

/**
 * NotFilter necessary. Use InvertExpressionVisitor
 */
public class NotFilter implements Filter, Serializable {

    private static final long serialVersionUID = 584860326604020881L;
    private Filter that;

    public NotFilter(Filter that) {
        this.that = that;
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return !that.satisfy(digest);
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
        return "NotFilter: " + that;
    }

}
