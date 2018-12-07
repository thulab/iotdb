package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LeafNode implements Node {

    private Reader reader;

    private BatchData data = null;

    private boolean gotData = false;

    public LeafNode(Reader reader, Path path, HashMap<Path, List<Reader>> readerCache) {
        this.reader = reader;

        if (!readerCache.containsKey(path))
            readerCache.put(path, new ArrayList<>());

        // put the current reader to valueCache
        readerCache.get(path).add(reader);
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

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }


}
