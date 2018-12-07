package cn.edu.tsinghua.iotdb.query.management;

import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter structure consist of all possible filters.
 */
public class FilterStructure {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterStructure.class);
    private GlobalTimeFilter timeFilter;
    private QueryFilter valueFilter, frequencyFilter;

    public FilterStructure(GlobalTimeFilter timeFilter, QueryFilter frequencyFilter, QueryFilter valueFilter) {

        this.timeFilter = timeFilter;
        this.valueFilter = valueFilter;
        this.frequencyFilter = frequencyFilter;
    }

    public GlobalTimeFilter getTimeFilter() {
        return this.timeFilter;
    }

    public QueryFilter getValueFilter() {
        return this.valueFilter;
    }

    public SeriesFilter getFrequencyFilter() {
        return (SeriesFilter) this.frequencyFilter;
    }

    public boolean noFilter() {
        return frequencyFilter == null && valueFilter == null && timeFilter == null;
    }

    public boolean onlyHasTimeFilter() {
        return this.timeFilter != null && this.valueFilter == null && this.frequencyFilter == null;
    }
}
