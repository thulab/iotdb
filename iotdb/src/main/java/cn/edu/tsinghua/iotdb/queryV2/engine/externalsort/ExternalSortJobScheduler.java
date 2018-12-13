package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;


public class ExternalSortJobScheduler {

    private long jobId = 0;

    private ExternalSortJobScheduler() {

    }

    public synchronized long genJobId() {
        jobId++;
        return jobId;
    }

    private static class ExternalSortJobSchedulerHelper {
        private static ExternalSortJobScheduler INSTANCE = new ExternalSortJobScheduler();
    }

    public static ExternalSortJobScheduler getInstance() {
        return ExternalSortJobSchedulerHelper.INSTANCE;
    }
}
