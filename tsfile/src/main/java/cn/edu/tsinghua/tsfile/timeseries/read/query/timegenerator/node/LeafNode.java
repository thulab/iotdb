package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.series.SeriesReader;

import java.io.IOException;

public class LeafNode implements Node {

    private SeriesReader reader;

    private BatchData data = null;

    private boolean gotData = false;

    public LeafNode(SeriesReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() throws IOException {

        if (gotData) {
            data.next();
            gotData = false;
        }

        if (data == null || !data.hasNext()) {
            if (reader.hasNextBatch())
                data = reader.nextBatch();
            else
                return false;
        }


        return data.hasNext();
    }

    @Override
    public long next() {
        long time = data.currentTime();
        gotData = true;
        return time;
    }

    public boolean currentTimeIs(long time) {
        if(!reader.currentBatch().hasNext())
            return false;
        return reader.currentBatch().currentTime() == time;
    }

    public Object currentValue(long time) {
        if(data.currentTime() == time)
            return data.currentValue();
        return null;
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }


}
