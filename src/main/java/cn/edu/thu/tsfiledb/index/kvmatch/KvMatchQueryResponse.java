package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.index.QueryResponse;

import java.util.List;

/**
 * The instance of this class represents a query response with specific candidates.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryResponse extends QueryResponse {

    public KvMatchQueryResponse() {
        super();
    }

    public KvMatchQueryResponse(List<Pair<Long, Long>> candidateRanges) {
        super(candidateRanges);
    }
}
