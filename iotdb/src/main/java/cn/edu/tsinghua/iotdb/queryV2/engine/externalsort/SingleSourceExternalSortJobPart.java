package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;


public class SingleSourceExternalSortJobPart extends ExternalSortJobPart {

    private PriorityTimeValuePairReader timeValuePairReader;

    public SingleSourceExternalSortJobPart(PriorityTimeValuePairReader timeValuePairReader) {
        super(ExternalSortJobPartType.SINGLE_SOURCE);
        this.timeValuePairReader = timeValuePairReader;
    }

    @Override
    public PriorityTimeValuePairReader execute() {
        return this.timeValuePairReader;
    }
}
