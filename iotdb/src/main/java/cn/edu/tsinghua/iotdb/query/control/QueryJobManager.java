package cn.edu.tsinghua.iotdb.query.control;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueryJobManager {

    /** to store all queryJobs in one query **/
    private static ThreadLocal<Set<Long>> queryJobIds = new ThreadLocal<>();
    private OpenedFileStreamManager openedFileStreamManager;

    private AtomicLong jobId;

    private QueryJobManager(){
        jobId = new AtomicLong(0L);
        openedFileStreamManager = OpenedFileStreamManager.getInstance();
    }

    private static class QueryJobManagerHolder {
        private static final QueryJobManager INSTANCE = new QueryJobManager();
    }

    public static QueryJobManager getInstance() {
        return QueryJobManager.QueryJobManagerHolder.INSTANCE;
    }

    public synchronized long addJobForOneQuery() {
        long jobIdCurrent = jobId.incrementAndGet();

        if (queryJobIds.get() == null) {
            queryJobIds.set(new HashSet<>());
        }
        queryJobIds.get().add(jobIdCurrent);

        return jobIdCurrent;
    }

    /**
     * Always invoking this method when jdbc connection close.
     */
    public void closeOneJobForOneQuery(long jobId) throws IOException {
    }

    public void closeAllJobForOneQuery() throws IOException {
    }

}
