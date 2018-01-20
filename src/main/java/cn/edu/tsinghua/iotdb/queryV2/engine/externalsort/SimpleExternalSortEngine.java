package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class SimpleExternalSortEngine implements ExternalSortJobEngine {

    private ExternalSortJobScheduler scheduler;
    //TODO: using config
    private String baseDir = "src/main/resources/externalSortTmp/";
    private int minExternalSortSourceCount = 10;

    private SimpleExternalSortEngine() {
        scheduler = ExternalSortJobScheduler.getInstance();
    }

    public SimpleExternalSortEngine(String baseDir, int minExternalSortSourceCount) {
        this.baseDir = baseDir;
        this.minExternalSortSourceCount = minExternalSortSourceCount;
        scheduler = ExternalSortJobScheduler.getInstance();
    }

    @Override
    public List<PriorityTimeValuePairReader> execute(List<PriorityTimeValuePairReader> readers) throws IOException {
        if (readers.size() < minExternalSortSourceCount) {
            return readers;
        }
        ExternalSortJob job = createJob(readers);
        return job.execute();
    }

    //TODO: this method could be optimized to have a better performance
    @Override
    public ExternalSortJob createJob(List<PriorityTimeValuePairReader> readers) {
        long jodId = scheduler.genJobId();
        List<ExternalSortJobPart> ret = new LinkedList<>();
        List<ExternalSortJobPart> tmpPartList = new ArrayList<>();
        for (PriorityTimeValuePairReader reader : readers) {
            ret.add(new SingleSourceExternalSortJobPart(reader));
        }

        int partId = 0;
        while (ret.size() >= minExternalSortSourceCount) {
            for (int i = 0; i < ret.size(); ) {
                List<ExternalSortJobPart> partGroup = new ArrayList<>();
                for (int j = 0; j < minExternalSortSourceCount && i < ret.size(); j++) {
                    partGroup.add(ret.get(i));
                    i++;
                }
                StringBuilder tmpFilePath = new StringBuilder(baseDir).append(jodId).append("_").append(partId);
                MultiSourceExternalSortJobPart part = new MultiSourceExternalSortJobPart(tmpFilePath.toString(), partGroup);
                tmpPartList.add(part);
                partId++;
            }
            ret = tmpPartList;
            tmpPartList = new ArrayList<>();
        }
        return new ExternalSortJob(jodId, ret);
    }

    private static class SimpleExternalSortJobEngineHelper {
        private static SimpleExternalSortEngine INSTANCE = new SimpleExternalSortEngine();
    }

    public SimpleExternalSortEngine getInstance() {
        return SimpleExternalSortJobEngineHelper.INSTANCE;
    }
}
