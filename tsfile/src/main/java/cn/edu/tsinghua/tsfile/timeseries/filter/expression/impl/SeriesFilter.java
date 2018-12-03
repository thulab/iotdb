package cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl;

import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.UnaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;


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
