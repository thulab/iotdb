package cn.edu.tsinghua.tsfile.read.expression.impl;

import cn.edu.tsinghua.tsfile.read.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.read.expression.UnaryQueryFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;


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
