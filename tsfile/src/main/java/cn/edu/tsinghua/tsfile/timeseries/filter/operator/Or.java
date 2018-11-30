package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.BinaryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.Serializable;

/**
 * Either of the left and right operators of And must satisfy the condition.
 */
public class Or extends BinaryFilter implements Serializable {

    private static final long serialVersionUID = -968055896528472694L;

    public Or(Filter left, Filter right) {
        super(left, right);
    }

    @Override
    public String toString() {
        return "(" + left + " || " + right + ")";
    }


    @Override
    public boolean satisfy(DigestForFilter digest) {
        return left.satisfy(digest) || right.satisfy(digest);
    }

    @Override
    public boolean satisfy(TimeValuePair pair) {
        return left.satisfy(pair) || right.satisfy(pair);
    }

    @Override
    public boolean satisfy(Object time, Object value) {
        return left.satisfy(time, value) || right.satisfy(time, value);
    }

}
