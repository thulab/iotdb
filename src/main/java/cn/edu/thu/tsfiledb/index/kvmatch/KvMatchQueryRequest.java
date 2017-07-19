package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.index.QueryRequest;

import java.util.List;

/**
 * An instance of this class represents a query request with specific parameters.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryRequest extends QueryRequest {

    private double epsilon;

    private double alpha;

    private double beta;

    /**
     * Private constructor used by the nested Builder class.
     *
     * @param builder builder used to create this query request
     */
    private KvMatchQueryRequest(final Builder builder) {
        super(builder.columnPath, builder.startTime, builder.endTime, builder.querySeries);
        this.epsilon = builder.epsilon;
        this.alpha = builder.alpha;
        this.beta = builder.beta;
    }

    public KvMatchQueryRequest(Path columnPath, long startTime, long endTime, List<Pair<Long, Double>> querySeries, double epsilon) {
        this(columnPath, startTime, endTime, querySeries, epsilon, 1.0, 0.0);
    }

    public KvMatchQueryRequest(Path columnPath, List<Pair<Long, Double>> querySeries, double epsilon, double alpha, double beta) {
        this(columnPath, Long.MIN_VALUE, Long.MAX_VALUE, querySeries, epsilon, alpha, beta);
    }

    public KvMatchQueryRequest(Path columnPath, long startTime, long endTime, List<Pair<Long, Double>> querySeries, double epsilon, double alpha, double beta) {
        super(columnPath, startTime, endTime, querySeries);
        this.epsilon = epsilon;
        this.alpha = alpha;
        this.beta = beta;
    }

    /**
     * Returns a {@link KvMatchQueryRequest.Builder} to create an {@link KvMatchQueryRequest} using descriptive methods.
     *
     * @return a new {@link KvMatchQueryRequest.Builder} instance
     */
    public static KvMatchQueryRequest.Builder builder(Path columnPath, List<Pair<Long, Double>> querySeries, double epsilon) {
        return new Builder(columnPath, querySeries, epsilon);
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

    /**
     * A nested builder class to create <code>KvMatchQueryRequest</code> instances using descriptive methods.
     * <p>
     * Example usage:
     * <pre>
     * KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, epsilon)
     *                                                       .alpha(1.0)
     *                                                       .beta(0.0)
     *                                                       .startTime(1500350823)
     *                                                       .endTime(1500350823)
     *                                                       .build();
     * </pre>
     */
    public static final class Builder {

        private Path columnPath;

        private long startTime;

        private long endTime;

        private List<Pair<Long, Double>> querySeries;

        private double epsilon;

        private double alpha;

        private double beta;

        /**
         * Constructs a new <code>Builder</code> with the minimum
         * required parameters for an <code>KvMatchQueryRequest</code> instance.
         *
         * @param columnPath  the column path request to query
         * @param querySeries the pattern series used to query
         * @param epsilon     the distance threshold
         * @throws IllegalArgumentException if there are any non valid arguments
         */
        private Builder(Path columnPath, List<Pair<Long, Double>> querySeries, double epsilon) throws IllegalArgumentException {
            if (columnPath == null || querySeries.isEmpty() || epsilon <= 0) {
                throw new IllegalArgumentException("The given query request is not valid!");
            }
            this.columnPath = columnPath;
            this.querySeries = querySeries;
            this.epsilon = epsilon;
            this.alpha = 1.0;
            this.beta = 0.0;
            this.startTime = Long.MIN_VALUE;
            this.endTime = Long.MAX_VALUE;
        }

        /**
         * Sets the parameter alpha for the query request
         *
         * @param alpha the parameter alpha for the query request
         * @return this builder, to allow method chaining
         */
        public Builder alpha(final double alpha) {
            this.alpha = alpha;
            return this;
        }

        /**
         * Sets the parameter beta for the query request
         *
         * @param beta the parameter alpha for the query request
         * @return this builder, to allow method chaining
         */
        public Builder beta(final double beta) {
            this.beta = beta;
            return this;
        }

        /**
         * Sets the start time for the query request
         *
         * @param startTime the start time for the query request
         * @return this builder, to allow method chaining
         */
        public Builder startTime(final long startTime) {
            this.startTime = startTime;
            return this;
        }

        /**
         * Sets the end time for the query request
         *
         * @param endTime the end time for the query request
         * @return this builder, to allow method chaining
         */
        public Builder endTime(final long endTime) {
            this.endTime = endTime;
            return this;
        }

        /**
         * Constructs an {@link KvMatchQueryRequest} with the values declared by this {@link KvMatchQueryRequest.Builder}.
         *
         * @return the new {@link KvMatchQueryRequest}
         * @throws IllegalArgumentException if either required arguments is illegal or has been set
         */
        public KvMatchQueryRequest build() {
            if (columnPath == null || querySeries.isEmpty() || epsilon <= 0 ||
                    alpha < 1.0 || beta < 0 || startTime > endTime) {
                throw new IllegalArgumentException("The given query request is not valid!");
            }
            return new KvMatchQueryRequest(this);
        }
    }
}
