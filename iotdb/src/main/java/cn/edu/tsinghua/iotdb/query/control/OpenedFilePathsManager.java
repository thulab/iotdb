package cn.edu.tsinghua.iotdb.query.control;

import cn.edu.tsinghua.iotdb.engine.filenode.IntervalFileNode;
import cn.edu.tsinghua.iotdb.engine.querycontext.OverflowInsertFile;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p> Singleton pattern, to manage all query tokens.
 * Each jdbc query request can query multiple series, in the processing of querying different device id,
 * the <code>FileNodeManager.getInstance().beginQuery</code> and <code>FileNodeManager.getInstance().endQuery</code>
 * must be invoked in the beginning and ending of jdbc request.
 *
 */
public class OpenedFilePathsManager {

    /**
     * Each jdbc request has unique jod id, job id is stored in thread local variable jobContainer.
     */
    private ThreadLocal<Long> jobContainer;

    /**
     * Map<jobId, Set<filePaths>>
     */
    private Map<Long, Set<String>> filePathsMap;

    private OpenedFilePathsManager() {
        jobContainer = new ThreadLocal<>();
        filePathsMap = new HashMap<>();
    }

    private static class QueryTokenManagerHelper {
        public static OpenedFilePathsManager INSTANCE = new OpenedFilePathsManager();
    }

    public static OpenedFilePathsManager getInstance() {
        return QueryTokenManagerHelper.INSTANCE;
    }

    /**
     * Set job id for current request thread.
     */
    public void setJobIdForCurrentRequestThread(long jobId) {
        jobContainer.set(jobId);
        filePathsMap.put(jobId, new HashSet<>());
    }

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

    public void removeUsedFilesForCurrentRequestThread() {
        if (jobContainer.get() != null) {
            long jobId = jobContainer.get();
            for (String filePath : filePathsMap.get(jobId)) {
                FileReaderManager.getInstance().decreaseFileReference(filePath);
            }
            filePathsMap.remove(jobId);
            jobContainer.remove();
        }
    }

    private void addFilePathToMap(long jobId, String filePath) {
        if (!filePathsMap.get(jobId).contains(filePath)) {
            filePathsMap.get(jobId).add(filePath);
            FileReaderManager.getInstance().increaseFileReference(filePath);
        }
    }
}
