package cn.edu.tsinghua.tsfile.read.expression.impl;

import cn.edu.tsinghua.tsfile.read.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.read.expression.UnaryQueryFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.common.Path;


public class SeriesFilter implements UnaryQueryFilter {
    private Path seriesPath;
    private Filter filter;

    public SeriesFilter(Path seriesDescriptor, Filter filter) {
        this.seriesPath = seriesDescriptor;
        this.filter = filter;
    }

    @Override
    public QueryFilterType getType() {
        return QueryFilterType.SERIES;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public String toString() {
        return "[" + seriesPath + ":" + filter + "]";
    }

    public Path getSeriesPath() {
        return this.seriesPath;
    }
}
