package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;
import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

import java.util.*;

/**
 * <p> Singleton pattern, to manage all query tokens.
 * Each jdbc query request can query multiple series, in the processing of querying different device id,
 * the <code>FileNodeManager.getInstance().beginQuery</code> and <code>FileNodeManager.getInstance().endQuery</code>
 * must be invoked in the beginning and ending of jdbc request.
 *
 */
public class QueryTokenManager {

    /**
     * Each jdbc request has unique jod id, job id is stored in thread local variable jobContainer.
     */
    private ThreadLocal<Long> jobContainer;

    /**
     * Map<jobId, Map<deviceId, List<token>>
     */
    private Map<Long, Map<String, List<Integer>>> tokensMap;

    private QueryTokenManager() {
        jobContainer = new ThreadLocal<>();
        tokensMap = new HashMap<>();
    }

    private static class QueryTokenManagerHelper {
        public static QueryTokenManager INSTANCE = new QueryTokenManager();
    }

    public static QueryTokenManager getInstance() {
        return QueryTokenManagerHelper.INSTANCE;
    }

    /**
     * Begin query and set query tokens of queryPaths.
     */
    public void beginQueryOfGivenQueryPaths(long jobId, List<Path> queryPaths) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        for (Path path : queryPaths) {
            deviceIdSet.add(path.getDevice());
        }
        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Begin query and set query tokens of all paths in expression.
     */
    public void beginQueryOfGivenExpression(long jobId, IExpression expression) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        getUniquePaths(expression, deviceIdSet);
        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Set job id for current request thread.
     */
    public void setJobIdForCurrentRequestThread(long jobId) {
        jobContainer.set(jobId);
        tokensMap.put(jobId, new HashMap<>());
    }

    /**
     * Get job id for current request thread.
     */
    public void endQueryForCurrentRequestThread() throws FileNodeManagerException {
        if (jobContainer.get() == null) {
            return;
        }

        long jobId = jobContainer.get();

        if (tokensMap.containsKey(jobId)) {
            Map<String, List<Integer>> deviceTokenMap = tokensMap.get(jobId);
            for (Map.Entry<String, List<Integer>> entry : deviceTokenMap.entrySet()) {
                for (int token : entry.getValue()) {
                    FileNodeManager.getInstance().endQuery(entry.getKey(), token);
                }
            }
            tokensMap.remove(jobId);
        }

        jobContainer.remove();
    }

    private void getUniquePaths(IExpression expression, Set<String> deviceIdSet) {
        if (expression.getType() == ExpressionType.AND || expression.getType() == ExpressionType.OR) {
            getUniquePaths(((IBinaryExpression) expression).getLeft(), deviceIdSet);
            getUniquePaths(((IBinaryExpression) expression).getRight(), deviceIdSet);
        } else if (expression.getType() == ExpressionType.SERIES) {
            SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) expression;
            deviceIdSet.add(singleSeriesExp.getSeriesPath().getDevice());
        }
    }

    private void putQueryTokenForCurrentRequestThread(long jobId, String deviceId, int queryToken) {
        if (!tokensMap.get(jobId).containsKey(deviceId)) {
            tokensMap.get(jobId).put(deviceId, new ArrayList<>());
        }
        tokensMap.get(jobId).get(deviceId).add(queryToken);
    }

}
