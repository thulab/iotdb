package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node;

import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public class LeafNode implements Node {

    private Reader seriesReader;

    public LeafNode(Reader seriesReader) {
        this.seriesReader = seriesReader;
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public long next() throws IOException {
        return seriesReader.next().getTimestamp();
    }

    @Override
    public NodeType getType() {
        return NodeType.LEAF;
    }
}
