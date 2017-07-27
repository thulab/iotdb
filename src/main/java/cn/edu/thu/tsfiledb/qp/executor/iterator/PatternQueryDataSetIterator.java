package cn.edu.thu.tsfiledb.qp.executor.iterator;

import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;
import cn.edu.thu.tsfiledb.index.kvmatch.KvMatchIndexManager;
import cn.edu.thu.tsfiledb.index.kvmatch.KvMatchQueryRequest;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Result wrap for KV-match index query, only return fetchSize number of results in one batch.
 *
 * @author Jiaye Wu
 */
public class PatternQueryDataSetIterator implements Iterator<QueryDataSet> {

    private static KvMatchIndexManager kvMatchIndexManager = KvMatchIndexManager.getInstance();

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
            data = kvMatchIndexManager.query(queryRequest, fetchSize);
        } catch (IndexManagerException e) {
            throw new RuntimeException(e.getMessage());
        }
        noNext = true;
        return true;
    }

    @Override
    public QueryDataSet next() {
        return data;
    }
}