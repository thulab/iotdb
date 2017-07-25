package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.index.QueryResponse;

import java.util.List;

/**
 * An instance of this class represents a query response with specific candidates.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryResponse extends QueryResponse {

    private List<Pair<Pair<Long, Long>, Double>> answers;

    public KvMatchQueryResponse(List<Pair<Pair<Long, Long>, Double>> answers) {
        this.answers = answers;
    }

    public List<Pair<Pair<Long, Long>, Double>> getAnswers() {
        return answers;
    }

    public void setAnswers(List<Pair<Pair<Long, Long>, Double>> answers) {
        this.answers = answers;
    }
}
