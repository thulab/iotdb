package cn.edu.tsinghua.tsfile.timeseries.filter.operator;

import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;

public class None implements Filter {

    public static final None INSTANCE = new None();

    @Override
    public boolean satisfy(DigestForFilter digest) {
        return true;
    }

    @Override
    public boolean satisfy(long time, Object value) {
        return true;
    }
}
