package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class LeafNode implements Node {

    private SeriesReader seriesReader;

    private BatchData data = null;

    private boolean gotData = false;

    public LeafNode(Reader seriesReader, Path path, HashMap<Path, List<SeriesReader>> readerCache) {
        this.seriesReader = (SeriesReader) seriesReader;

        if(!readerCache.containsKey(path))
            readerCache.put(path, new ArrayList<>());

        // put the current reader to valueCache
        readerCache.get(path).add((SeriesReader) seriesReader);
    }

    @Override
    public boolean hasNext() throws IOException {

        if(gotData)
            data.next();

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
        gotData = true;
        return time;
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }


}
