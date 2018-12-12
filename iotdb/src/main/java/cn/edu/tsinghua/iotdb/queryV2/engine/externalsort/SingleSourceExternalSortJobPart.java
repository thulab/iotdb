package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.merge.PrioritySeriesReader;


public class SingleSourceExternalSortJobPart extends ExternalSortJobPart {

    private PrioritySeriesReader timeValuePairReader;

    public SingleSourceExternalSortJobPart(PrioritySeriesReader timeValuePairReader) {
        super(ExternalSortJobPartType.SINGLE_SOURCE);
        this.timeValuePairReader = timeValuePairReader;
    }

    @Override
    public PrioritySeriesReader execute() {
        return this.timeValuePairReader;
    }
}
