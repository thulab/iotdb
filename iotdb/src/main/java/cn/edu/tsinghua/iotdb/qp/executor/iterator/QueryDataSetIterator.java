package cn.edu.tsinghua.iotdb.qp.executor.iterator;

import cn.edu.tsinghua.iotdb.qp.executor.QueryProcessExecutor;
import cn.edu.tsinghua.iotdb.query.management.FilterStructure;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.common.RowRecord;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class QueryDataSetIterator implements QueryDataSet {

    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetIterator.class);
    private final int fetchSize;
    private final QueryProcessExecutor executor;
    private QueryDataSet data = null;
    private List<Path> paths;
    private List<String> aggregations;
    private List<FilterStructure> filterStructures = new ArrayList<>();
    private boolean hasNext = true;

    //group by
    private long unit;
    private long origin;
    private List<Pair<Long, Long>> intervals;


    //groupby
    public QueryDataSetIterator(List<Path> paths, int fetchSize, List<String> aggregations,
                                List<FilterStructure> filterStructures, long unit, long origin,
                                List<Pair<Long, Long>> intervals, QueryProcessExecutor executor) {
        this.fetchSize = fetchSize;
        this.executor = executor;
        this.filterStructures = filterStructures;
        this.paths = paths;
        this.aggregations = aggregations;
        this.unit = unit;
        this.origin = origin;
        this.intervals = intervals;
    }


    @Override
    public boolean hasNext() throws IOException {
        if(!hasNext)
            return false;
        if (data == null || !data.hasNext()) {
            try {
                data = executor.groupBy(getAggrePair(), filterStructures, unit, origin, intervals, fetchSize);
                if(data.hasNext()) {
                    return true;
                } else {
                    hasNext = false;
                    return false;
                }
            } catch (Exception e) {
                logger.error("meet error in hasNext because: "+ e.getMessage());
            }
        }
        return true;
    }

    private List<Pair<Path, String>> getAggrePair() {
        List<Pair<Path, String>> aggres = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            if (paths.size() == aggregations.size()) {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(i)));
            } else {
                aggres.add(new Pair<>(paths.get(i), aggregations.get(0)));
            }
        }
        return aggres;
    }

    @Override
    public RowRecord next() throws IOException {
        return data.next();
    }

}
