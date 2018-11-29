package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.BinaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

/**
 * Both the left and right operators of And must satisfy the condition.
 */
public class And extends BinaryFilter {

    private static final long serialVersionUID = 6705254093824897938L;

    public And(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return left.satisfy(digest) && right.satisfy(digest);
    }

    @Override
    public boolean satisfy(TimeValuePair pair) {
        return left.satisfy(pair) && right.satisfy(pair);
    }

    @Override
    public boolean satisfy(Object time, Object value) {
        return left.satisfy(time, value) && right.satisfy(time, value);
    }

    @Override
    public String toString() {
        return "(" + left + " && " + right + ")";
    }
}
