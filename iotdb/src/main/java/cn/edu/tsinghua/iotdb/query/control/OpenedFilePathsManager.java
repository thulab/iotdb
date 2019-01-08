package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p> Singleton pattern, to manage all query tokens.
 * Each jdbc query request can query multiple series, in the processing of querying different device id,
 * the <code>FileNodeManager.getInstance().beginQuery</code> and <code>FileNodeManager.getInstance().endQuery</code>
 * must be invoked in the beginning and ending of jdbc request.
 *
 */
public class OpenedFilePathsManager {

    /**
     * Each jdbc request has an unique jod id, job id is stored in thread local variable jobIdContainer.
     */
    private ThreadLocal<Long> jobIdContainer;

    /**
     * Map<jobId, Set<filePaths>>
     */
    private ConcurrentHashMap<Long, Set<String>> filePathsMap;

    /**
     * Set job id for current request thread.
     * When a query request is created firstly, this method must be invoked.
     */
    public void setJobIdForCurrentRequestThread(long jobId) {
        jobIdContainer.set(jobId);
        filePathsMap.put(jobId, new HashSet<>());
    }

    /**
     * Add the unique file paths to filePathsMap.
     */
    public void addUsedFilesForCurrentRequestThread(long jobId, QueryDataSource dataSource) {
        for (IntervalFileNode intervalFileNode : dataSource.getSeqDataSource().getSealedTsFiles()) {
            String sealedFilePath = intervalFileNode.getFilePath();
            addFilePathToMap(jobId, sealedFilePath);
        }

        if (dataSource.getSeqDataSource().hasUnsealedTsFile()) {
            String unSealedFilePath = dataSource.getSeqDataSource().getUnsealedTsFile().getFilePath();
            addFilePathToMap(jobId, unSealedFilePath);
        }

        for (OverflowInsertFile overflowInsertFile : dataSource.getOverflowSeriesDataSource().getOverflowInsertFileList()) {
            String overflowFilePath = overflowInsertFile.getFilePath();
            addFilePathToMap(jobId, overflowFilePath);
        }
    }

    /**
     * Whenever the jdbc request is closed normally or abnormally, this method must be invoked.
     * All file paths used by this jdbc request must be cleared and thus the usage reference must be decreased.
     */
    public void removeUsedFilesForCurrentRequestThread() {
        if (jobIdContainer.get() != null) {
            long jobId = jobIdContainer.get();
            jobIdContainer.remove();

            for (String filePath : filePathsMap.get(jobId)) {
                FileReaderManager.getInstance().decreaseFileReaderReference(filePath);
            }
            filePathsMap.remove(jobId);
        }
    }

    /**
     * Increase the usage reference of filePath of job id.
     * Before the invoking of this method, <code>this.setJobIdForCurrentRequestThread</code> has been invoked,
     * so <code>filePathsMap.get(jobId)</code> must not return null.
     */
    public void addFilePathToMap(long jobId, String filePath) {
        if (!filePathsMap.get(jobId).contains(filePath)) {
            filePathsMap.get(jobId).add(filePath);
            FileReaderManager.getInstance().increaseFileReaderReference(filePath);
        }
    }

    private OpenedFilePathsManager() {
        jobIdContainer = new ThreadLocal<>();
        filePathsMap = new ConcurrentHashMap<>();
    }

    private static class QueryTokenManagerHelper {
        public static OpenedFilePathsManager INSTANCE = new OpenedFilePathsManager();
    }

    public static OpenedFilePathsManager getInstance() {
        return QueryTokenManagerHelper.INSTANCE;
    }
}
