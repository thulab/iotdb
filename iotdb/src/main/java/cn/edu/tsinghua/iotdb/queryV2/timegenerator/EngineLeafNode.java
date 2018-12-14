package cn.edu.tsinghua.iotdb.queryV2.timegenerator;

import cn.edu.tsinghua.iotdb.queryV2.reader.IReader;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.NodeType;


public class EngineLeafNode implements Node {

    private IReader reader;

    private BatchData data = null;

    private boolean gotData = false;

    public EngineLeafNode(IReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean hasNext() {

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
