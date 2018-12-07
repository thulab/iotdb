package cn.edu.tsinghua.iotdb.query.management;

import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter structure consist of all possible filters.
 */
public class FilterStructure {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterStructure.class);
    private SeriesFilter timeFilter;
    private FilterExpression valueFilter, frequencyFilter;

    public FilterStructure(FilterExpression timeFilter, FilterExpression frequencyFilter, FilterExpression valueFilter) {
        if (timeFilter != null && !(timeFilter instanceof SeriesFilter)) {
            LOGGER.error("Time filter is not single!");
            return;
        }

        this.timeFilter = (SeriesFilter) timeFilter;
        this.valueFilter = valueFilter;
        this.frequencyFilter = frequencyFilter;
    }

    public SeriesFilter getTimeFilter() {
        return this.timeFilter;
    }

    public FilterExpression getValueFilter() {
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
