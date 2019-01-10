package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;
import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
     *
     * <p>
     * Key of queryTokensMap is job id, value of queryTokensMap is a deviceId-tokenList map,
     * key of the deviceId-tokenList map is device id, value of deviceId-tokenList map is a list of tokens.
     * </p>
     *
     * <p>
     * For example, in a query process Q1, we give a query sql <sql>select device_1.sensor_1, device_1.sensor_2, device_2.sensor_1, device_2.sensor_2</sql>,
     * in this query clause, we will invoke <code>FileNodeManager.getInstance().beginQuery(device_1)</code> once and
     * invoke <code>FileNodeManager.getInstance().beginQuery(device_2)</code> once. Notice that, Although in the query sql, there exists four paths, but the unique
     * devices are only `device_1` and `device_2`.
     * Assuming that in the invoking of <code>FileNodeManager.getInstance().beginQuery(device_1)</code>, it returns a
     * result token `1` and the invoking <code>FileNodeManager.getInstance().beginQuery(device_2)</code>, it returns a
     * another result token `2`;
     *
     * Assume that, in the meanwhile, in another client, a query process Q2,
     * we also give a query sql <sql>select device_1.sensor_1, device_1.sensor_2, device_2.sensor_1, device_2.sensor_2</sql>,
     * in this query clause, we will invoke <code>FileNodeManager.getInstance().beginQuery(device_1)</code> once and
     * invoke <code>FileNodeManager.getInstance().beginQuery(device_2)</code> once.
     * Assume that, in the first invoking of <code>FileNodeManager.getInstance().beginQuery(device_1)</code>, it returns a
     * result token `3` and in the invoking of <code>FileNodeManager.getInstance().beginQuery(device_2)</code>, it returns a
     * another result token `4`;
     *
     * As a right flow, in the exit of query process Q1 normally or abnormally, <code>FileNodeManager.getInstance().endQuery(device_1, 1)</code> and
     * <code>FileNodeManager.getInstance().endQuery(device_2, 2)</code> must be invoked. In the exit of  query process Q2 normally or abnormally,
     * <code>FileNodeManager.getInstance().endQuery(device_1, 3)</code> and <code>FileNodeManager.getInstance().endQuery(device_2, 4)</code> must be invoked.
     *
     * Only when the invoking of <code>FileNodeManager.getInstance().beginQuery()</code> and <code>FileNodeManager.getInstance().endQuery()</code> is right,
     * the write process and query process of IoTDB can run rightly.
     * </p>
     */
    private ConcurrentHashMap<Long, ConcurrentHashMap<String, List<Integer>>> queryTokensMap;

    /**
     * Set job id for current request thread.
     * When a query request is created firstly, this method must be invoked.
     */
    public void setJobIdForCurrentRequestThread(long jobId) {
        jobContainer.set(jobId);
        queryTokensMap.put(jobId, new ConcurrentHashMap<>());
    }

    /**
     * Begin query and set query tokens of queryPaths.
     * This method is used for projection calculation.
     */
    public void beginQueryOfGivenQueryPaths(long jobId, List<Path> queryPaths) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        queryPaths.forEach((path) -> deviceIdSet.add(path.getDevice()));

        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Begin query and set query tokens of all paths in expression.
     * This method is used in filter calculation.
     */
    public void beginQueryOfGivenExpression(long jobId, IExpression expression) throws FileNodeManagerException {
        Set<String> deviceIdSet = new HashSet<>();
        getUniquePaths(expression, deviceIdSet);
        for (String deviceId : deviceIdSet) {
            putQueryTokenForCurrentRequestThread(jobId, deviceId, FileNodeManager.getInstance().beginQuery(deviceId));
        }
    }

    /**
     * Whenever the jdbc request is closed normally or abnormally, this method must be invoked.
     * All query tokens created by this jdbc request must be cleared.
     */
    public void endQueryForCurrentRequestThread() throws FileNodeManagerException {
        if (jobContainer.get() != null) {
            long jobId = jobContainer.get();
            jobContainer.remove();

            for (Map.Entry<String, List<Integer>> entry : queryTokensMap.get(jobId).entrySet()) {
                for (int token : entry.getValue()) {
                    FileNodeManager.getInstance().endQuery(entry.getKey(), token);
                }
            }
            queryTokensMap.remove(jobId);
        }
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
        if (!queryTokensMap.get(jobId).containsKey(deviceId)) {
            queryTokensMap.get(jobId).put(deviceId, new ArrayList<>());
        }
        queryTokensMap.get(jobId).get(deviceId).add(queryToken);
    }

    private QueryTokenManager() {
        jobContainer = new ThreadLocal<>();
        queryTokensMap = new ConcurrentHashMap<>();
    }

    private static class QueryTokenManagerHelper {
        public static QueryTokenManager INSTANCE = new QueryTokenManager();
    }

    public static QueryTokenManager getInstance() {
        return QueryTokenManagerHelper.INSTANCE;
    }
}
