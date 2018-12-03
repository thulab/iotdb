package cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.UnaryQueryFilter;


public class GlobalTimeFilter implements UnaryQueryFilter {
    private Filter filter;

    public GlobalTimeFilter(Filter filter) {
        this.filter = filter;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    @Override
    public QueryFilterType getType() {
        return QueryFilterType.GLOBAL_TIME;
    }

    public String toString() {
        return "[" + this.filter.toString() + "]";
    }
}
