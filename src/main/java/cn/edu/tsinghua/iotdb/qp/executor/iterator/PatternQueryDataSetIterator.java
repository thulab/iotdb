package cn.edu.tsinghua.iotdb.qp.executor.iterator;

import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.index.IoTIndex;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.index.kvmatch.KvMatchQueryRequest;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static cn.edu.tsinghua.iotdb.index.IndexManager.IndexType.KvIndex;

/**
 * Result wrap for KV-match index query, only return fetchSize number of results in one batch.
 *
 * @author Jiaye Wu
 */
public class PatternQueryDataSetIterator implements Iterator<QueryDataSet> {

    private static IoTIndex kvMatchIndexManager = IndexManager.getIndexInstance(KvIndex);

    private boolean noNext = false;
    private KvMatchQueryRequest queryRequest;
    private final int fetchSize;
    private QueryDataSet data = null;

    public PatternQueryDataSetIterator(KvMatchQueryRequest queryRequest, int fetchSize) {
        this.queryRequest = queryRequest;
        this.fetchSize = fetchSize;
    }

    @Override
    public boolean hasNext() {
        if (noNext) {
            return false;
        }
        try {
            List<Object> parameters = new ArrayList<>();
            parameters.add(queryRequest.getStartTime());
            parameters.add(queryRequest.getEndTime());
            parameters.add(queryRequest.getQueryPath());
            parameters.add(queryRequest.getQueryStartTime());
            parameters.add(queryRequest.getQueryEndTime());
            parameters.add(queryRequest.getEpsilon());
            parameters.add(queryRequest.getAlpha());
            parameters.add(queryRequest.getBeta());
            data = (QueryDataSet) kvMatchIndexManager.query(queryRequest.getColumnPath(), parameters, null, fetchSize);
        } catch (IndexManagerException e) {
            throw new RuntimeException(e.getMessage());
        }
        noNext = true;
        return data.hasNextRecord();
    }

    @Override
    public QueryDataSet next() {
        return data;
    }
}