package cn.edu.tsinghua.tsfile.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.read.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.read.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.read.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class TimeGeneratorImpl implements TimeGenerator {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;
    private Node operatorNode;

    private HashMap<Path, List<LeafNode>> leafCache;

    public TimeGeneratorImpl(QueryFilter queryFilter, ChunkLoader chunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
        this.leafCache = new HashMap<>();

        operatorNode = construct(queryFilter);
    }

    @Override
    public boolean hasNext() throws IOException {
        return operatorNode.hasNext();
    }

    @Override
    public long next() throws IOException {
        return operatorNode.next();
    }

    @Override
    public Object getValue(Path path, long time) {

        for (LeafNode leafNode : leafCache.get(path)) {
            if(!leafNode.currentTimeIs(time))
                continue;
            return leafNode.currentValue(time);
        }

        return null;
    }


    /**
     * construct the tree that generate timestamp
     */
    private Node construct(QueryFilter queryFilter) throws IOException {

        if (queryFilter.getType() == QueryFilterType.SERIES) {
            SeriesFilter seriesFilter = (SeriesFilter) queryFilter;
            SeriesReader seriesReader = generateSeriesReader(seriesFilter);
            Path path = seriesFilter.getSeriesPath();

            if (!leafCache.containsKey(path))
                leafCache.put(path, new ArrayList<>());

            // put the current reader to valueCache
            LeafNode leafNode = new LeafNode(seriesReader);
            leafCache.get(path).add(leafNode);

            return leafNode;

        } else if (queryFilter.getType() == QueryFilterType.OR) {
            Node leftChild = construct(((BinaryQueryFilter) queryFilter).getLeft());
            Node rightChild = construct(((BinaryQueryFilter) queryFilter).getRight());
            return new OrNode(leftChild, rightChild);

        } else if (queryFilter.getType() == QueryFilterType.AND) {
            Node leftChild = construct(((BinaryQueryFilter) queryFilter).getLeft());
            Node rightChild = construct(((BinaryQueryFilter) queryFilter).getRight());
            return new AndNode(leftChild, rightChild);
        }
        throw new UnSupportedDataTypeException("Unsupported QueryFilterType when construct OperatorNode: " + queryFilter.getType());
    }

    private SeriesReader generateSeriesReader(SeriesFilter seriesFilter) throws IOException {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(
                seriesFilter.getSeriesPath());
        return new SeriesReaderWithFilter(chunkLoader, chunkMetaDataList, seriesFilter.getFilter());
    }
}
