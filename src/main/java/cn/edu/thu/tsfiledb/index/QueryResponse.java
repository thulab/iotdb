package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * The abstract class for a query response with specific candidates.
 *
 * @author Jiaye Wu
 */
public abstract class QueryResponse {

    protected List<Pair<Long, Long>> candidateRanges;

    protected QueryResponse() {
        this.candidateRanges = new ArrayList<>();
    }

    protected QueryResponse(List<Pair<Long, Long>> candidateRanges) {
        this.candidateRanges = candidateRanges;
    }

    public List<Pair<Long, Long>> getCandidateRanges() {
        return candidateRanges;
    }

    public void setCandidateRanges(List<Pair<Long, Long>> candidateRanges) {
        this.candidateRanges = candidateRanges;
    }
}
