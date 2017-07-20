package cn.edu.thu.tsfiledb.index.kvmatch.support;

import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.QueryResponse;
import cn.edu.thu.tsfiledb.index.kvmatch.KvMatchQueryResponse;

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
        // 1. fetch non-indexed ranges from overflow manager

        // 2. fetch TsFile data file list from file node manager

        // 3. search corresponding index files of data files in the query range

        // 4. merge the candidate ranges and non-indexed ranges to produce candidate ranges

        // 5. scan the data in candidate ranges and find out actual answers

        return new KvMatchQueryResponse();
    }
}
