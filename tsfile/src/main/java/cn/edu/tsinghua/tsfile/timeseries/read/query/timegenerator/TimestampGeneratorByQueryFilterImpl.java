package cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.timeseries.read.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;


public class TimestampGeneratorByQueryFilterImpl implements TimestampGenerator {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;
    private Node operatorNode;

    private HashMap<Path, List<Reader>> readerCache;

    public TimestampGeneratorByQueryFilterImpl(QueryFilter queryFilter, ChunkLoader chunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
        this.readerCache = new HashMap<>();

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

        for (Reader reader : readerCache.get(path)) {
            if(!reader.nextBatch().hasNext())
                return null;
            if (reader.nextBatch().getTime() == time)
                return reader.nextBatch().getValue();
        }

        return null;
    }


    /**
     * construct the tree that generate timestamp
     */
    private Node construct(QueryFilter queryFilter) throws IOException {

        if (queryFilter.getType() == QueryFilterType.SERIES) {
            return new LeafNode(
                    generateSeriesReader((SeriesFilter) queryFilter),
                    ((SeriesFilter) queryFilter).getSeriesPath(),
                    readerCache);

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
