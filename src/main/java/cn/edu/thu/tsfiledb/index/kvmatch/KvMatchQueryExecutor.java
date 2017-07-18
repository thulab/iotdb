package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.QueryResponse;

/**
 * This is the class actually execute the KV-match index query processing.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryExecutor {

    private QueryRequest queryRequest;

    public KvMatchQueryExecutor(QueryRequest queryRequest) {
        this.queryRequest = queryRequest;
    }

    public QueryResponse execute() {
        return new KvMatchQueryResponse();
    }
}
