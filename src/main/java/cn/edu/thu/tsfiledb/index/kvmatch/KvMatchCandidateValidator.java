package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.SeriesUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfiledb.index.common.QueryDataSetIterator;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The class validates the candidates to find out actual matching results satisfying the query request.
 *
 * @author Jiaye Wu
 */
public class KvMatchCandidateValidator implements Callable<List<Pair<Pair<Long, Long>, Double>>> {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchCandidateValidator.class);

    private Path columnPath;

    private List<Pair<Long, Long>> scanIntervals;

    private QueryConfig queryConfig;

    private int token;

    public KvMatchCandidateValidator(Path columnPath, List<Pair<Long, Long>> scanIntervals, QueryConfig queryConfig, int token) {
        // TODO: There is a bug! The path will be modified in query process. Have to make a copy.
        this.columnPath = new Path(columnPath.getFullPath());
        this.scanIntervals = scanIntervals;
        this.queryConfig = queryConfig;
        this.token = token;
    }

    @Override
    public List<Pair<Pair<Long, Long>, Double>> call() throws Exception {
        logger.info("Validating candidate intervals: {}", scanIntervals);

        QueryDataSetIterator queryDataSetIterator = new QueryDataSetIterator(new OverflowQueryEngine(), columnPath, scanIntervals, token);

        List<Pair<Pair<Long, Long>, Double>> result = new ArrayList<>();
        Pair<Long, Double> lastKeyPoint = null;
        for (Pair<Long, Long> scanInterval : scanIntervals) {
            List<Pair<Long, Double>> keyPoints = new ArrayList<>();
            while (queryDataSetIterator.hasNext()) {
                RowRecord row = queryDataSetIterator.getRowRecord();
                double value = Double.parseDouble(row.getFields().get(0).getStringValue());
                if (keyPoints.isEmpty() && row.getTime() > scanInterval.left) {
                    if (lastKeyPoint == null) {
                        keyPoints.add(new Pair<>(scanInterval.left, value));
                    } else {
                        keyPoints.add(lastKeyPoint);
                    }
                }
                keyPoints.add(new Pair<>(row.getTime(), value));
                if (row.getTime() >= scanInterval.right) break;
            }
            if (keyPoints.isEmpty()) break;
            lastKeyPoint = keyPoints.get(keyPoints.size() - 1);
            List<Double> series = SeriesUtils.amend(keyPoints, scanInterval);

            double ex = 0, ex2 = 0;
            int lenQ = queryConfig.getQuerySeries().size(), idx = 0;
            double[] T = new double[2 * lenQ];
            for (int i = 0; i < series.size(); i++) {
                double value = series.get(i);
                ex += value;
                ex2 += value * value;
                T[i % lenQ] = value;
                T[(i % lenQ) + lenQ] = value;

                if (i >= lenQ - 1) {
                    int j = (i + 1) % lenQ;  // the current starting location of T

                    long left = scanInterval.left + i - lenQ + 1;
                    if (left == keyPoints.get(idx).left) {  // remove non-exist timestamp
                        idx++;

                        if (queryConfig.isNormalization()) {
                            double mean = ex / lenQ;  // z
                            double std = Math.sqrt(ex2 / lenQ - mean * mean);

                            if (Math.abs(mean - queryConfig.getMeanQ()) <= queryConfig.getBeta() && std / queryConfig.getStdQ() <= queryConfig.getBeta() && std / queryConfig.getStdQ() >= 1.0 / queryConfig.getAlpha()) {
                                double dist = 0;
                                for (int k = 0; k < lenQ && dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon(); k++) {
                                    double x = (T[(queryConfig.getOrder().get(k) + j)] - mean) / std;
                                    dist += (x - queryConfig.getNormalizedQuerySeries().get(k)) * (x - queryConfig.getNormalizedQuerySeries().get(k));
                                }
                                if (dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon()) {
                                    result.add(new Pair<>(new Pair<>(left, scanInterval.left + i), Math.sqrt(dist)));
                                }
                            }
                        } else {
                            double dist = 0;
                            for (int k = 0; k < lenQ && dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon(); k++) {
                                double x = T[k + j];
                                dist += (x - queryConfig.getQuerySeries().get(k)) * (x - queryConfig.getQuerySeries().get(k));
                            }
                            if (dist <= queryConfig.getEpsilon() * queryConfig.getEpsilon()) {
                                result.add(new Pair<>(new Pair<>(left, scanInterval.left + i), Math.sqrt(dist)));
                            }
                        }
                    }

                    ex -= T[j];
                    ex2 -= T[j] * T[j];
                }
            }
        }
        logger.info("Finished validating candidate intervals: {}", scanIntervals);

        return result;
    }
}
