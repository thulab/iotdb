package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents an external sort job. Every job will use a separated directory.
 */
public class ExternalSortJob {
    private long jobId;
    private List<ExternalSortJobPart> partList;

    public ExternalSortJob(long jobId, List<ExternalSortJobPart> partList) {
        this.jobId = jobId;
        this.partList = partList;
    }

    public List<PriorityTimeValuePairReader> execute() throws IOException {
        List<PriorityTimeValuePairReader> readers = new ArrayList<>();
        for (ExternalSortJobPart part : partList) {
            readers.add(part.execute());
        }
        return readers;
    }
}
