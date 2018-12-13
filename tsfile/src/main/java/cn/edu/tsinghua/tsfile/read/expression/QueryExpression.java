package cn.edu.tsinghua.tsfile.read.expression;

import cn.edu.tsinghua.tsfile.read.common.Path;

import java.util.ArrayList;
import java.util.List;


public class QueryExpression {
    private List<Path> selectedSeries;
    private IExpression IExpression;
    private boolean hasQueryFilter;

    private QueryExpression() {
        selectedSeries = new ArrayList<>();
        hasQueryFilter = false;
    }

    public static QueryExpression create() {
        return new QueryExpression();
    }

    public static QueryExpression create(List<Path> selectedSeries, IExpression filter) {
        QueryExpression ret = new QueryExpression();
        ret.selectedSeries = selectedSeries;
        ret.IExpression = filter;
        ret.hasQueryFilter = filter != null;
        return ret;
    }

    public QueryExpression addSelectedPath(Path path) {
        this.selectedSeries.add(path);
        return this;
    }

    public QueryExpression setIExpression(IExpression IExpression) {
        if (IExpression != null) {
            this.IExpression = IExpression;
            hasQueryFilter = true;
        }
        return this;
    }

    public QueryExpression setSelectSeries(List<Path> selectedSeries) {
        this.selectedSeries = selectedSeries;
        return this;
    }

    public IExpression getIExpression() {
        return IExpression;
    }

    public List<Path> getSelectedSeries() {
        return selectedSeries;
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("\n\t[Selected Series]:").append(selectedSeries)
                .append("\n\t[IExpression]:").append(IExpression);
        return stringBuilder.toString();
    }

    public boolean hasQueryFilter() {
        return hasQueryFilter;
    }
}
