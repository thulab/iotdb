package cn.edu.tsinghua.tsfile.timeseries.filter;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.*;


public class TimeFilter {

    public static class TimeEq extends Eq {
        private TimeEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeNotEq extends NotEq {
        private TimeNotEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGt extends Gt {
        private TimeGt(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeGtEq extends GtEq {
        private TimeGtEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLt extends Lt {
        private TimeLt(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }

    public static class TimeLtEq extends LtEq {
        private TimeLtEq(long value) {
            super(value, FilterType.TIME_FILTER);
        }
    }


    public static class TimeNot extends Not {
        private TimeNot(Filter filter) {
            super(filter);
        }
    }

    public static TimeEq eq(long value) {
        return new TimeEq(value);
    }

    public static TimeGt gt(long value) {
        return new TimeGt(value);
    }

    public static TimeGtEq gtEq(long value) {
        return new TimeGtEq(value);
    }

    public static TimeLt lt(long value) {
        return new TimeLt(value);
    }

    public static TimeLtEq ltEq(long value) {
        return new TimeLtEq(value);
    }

    public static TimeNot not(Filter filter) {
        return new TimeNot(filter);
    }

    public static TimeNotEq notEq(long value) {
        return new TimeNotEq(value);
    }

}
