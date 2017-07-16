package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.index.QueryRequest;

import java.util.List;

/**
 * The instance of this class represents a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryRequest extends QueryRequest {

    private double epsilon;

    private double alpha;

    private double beta;

    public KvMatchQueryRequest(String nameSpacePath, long startTime, long endTime, List<Pair<Long, Double>> querySeries, double epsilon) {
        super(nameSpacePath, startTime, endTime, querySeries);
        this.epsilon = epsilon;
    }

    public KvMatchQueryRequest(String nameSpacePath, long startTime, long endTime, List<Pair<Long, Double>> querySeries, double epsilon, double alpha, double beta) {
        super(nameSpacePath, startTime, endTime, querySeries);
        this.epsilon = epsilon;
        this.alpha = alpha;
        this.beta = beta;
    }

    public KvMatchQueryRequest(String nameSpacePath, List<Pair<Long, Double>> querySeries, double epsilon, double alpha, double beta) {
        super(nameSpacePath, Long.MIN_VALUE, Long.MAX_VALUE, querySeries);
        this.epsilon = epsilon;
        this.alpha = alpha;
        this.beta = beta;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public double getBeta() {
        return beta;
    }

    public void setBeta(double beta) {
        this.beta = beta;
    }
}
