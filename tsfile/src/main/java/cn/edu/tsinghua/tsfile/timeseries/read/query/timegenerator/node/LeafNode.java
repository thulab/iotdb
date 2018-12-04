package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;


public class LeafNode implements Node {

    // TODO make next method implementation of seriesReader using DOC
    private Reader seriesReader;

    private DynamicOneColumnData data = null;

    public LeafNode(Reader seriesReader) {
        this.seriesReader = seriesReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(data == null || !data.hasNext()) {
            if(seriesReader.hasNextBatch())
                data = seriesReader.nextBatch();
            else
                return false;
        }

        return data.hasNext();
    }

    @Override
    public long next() {
        long time = data.getTime();
        data.next();
        return time;
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }


}
