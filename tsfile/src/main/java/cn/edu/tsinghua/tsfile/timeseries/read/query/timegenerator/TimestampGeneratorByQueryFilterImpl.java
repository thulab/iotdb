package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.Node;

import java.io.IOException;


public class TimestampGeneratorByQueryFilterImpl implements TimestampGenerator {

    private QueryFilter queryFilter;
    private Node operatorNode;

    public TimestampGeneratorByQueryFilterImpl(QueryFilter queryFilter, ChunkLoader chunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.queryFilter = queryFilter;
        initNode(chunkLoader, metadataQuerier);
    }

    private void initNode(ChunkLoader chunkLoader, MetadataQuerier metadataQuerier) throws IOException {
        NodeConstructorForSingleFileImpl nodeConstructorForSingleFile = new NodeConstructorForSingleFileImpl(metadataQuerier, chunkLoader);
        this.operatorNode = nodeConstructorForSingleFile.construct(queryFilter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }
}
