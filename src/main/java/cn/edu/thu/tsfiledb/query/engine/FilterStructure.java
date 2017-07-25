package cn.edu.thu.tsfiledb.query.engine;

import cn.edu.thu.tsfile.timeseries.filter.definition.CrossSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A filter structure consist of all possible filters.
 */
public class FilterStructure {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterStructure.class);
    private SingleSeriesFilterExpression timeFilter;
    private FilterExpression valueFilter, frequencyFilter;

    public FilterStructure(FilterExpression timeFilter, FilterExpression valueFilter, FilterExpression frequencyFilter) {
        if (!(timeFilter instanceof SingleSeriesFilterExpression)) {
            LOGGER.error("Time filter is not single!");
            return;
        }
        this.timeFilter = (SingleSeriesFilterExpression) timeFilter;
        this.valueFilter = valueFilter;
        this.frequencyFilter = frequencyFilter;
    }

    public SingleSeriesFilterExpression getTimeFilter() {
        return this.timeFilter;
    }

    public FilterExpression getValueFilter() {
        return this.valueFilter;
    }

    public SingleSeriesFilterExpression getFrequencyFilter() {
        return (SingleSeriesFilterExpression) this.frequencyFilter;
    }

    public boolean isValueFilterCross() {
        return valueFilter instanceof CrossSeriesFilterExpression;
    }
}
