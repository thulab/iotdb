package cn.edu.tsinghua.tsfile.read.query.timegenerator;

import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.expression.ExpressionType;
import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoader;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderWithFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class TimeGeneratorImpl implements TimeGenerator {

    private ChunkLoader chunkLoader;
    private MetadataQuerier metadataQuerier;
    private Node operatorNode;

    private HashMap<Path, List<LeafNode>> leafCache;

    public TimeGeneratorImpl(IExpression IExpression, ChunkLoader chunkLoader
            , MetadataQuerier metadataQuerier) throws IOException {
        this.chunkLoader = chunkLoader;
        this.metadataQuerier = metadataQuerier;
        this.leafCache = new HashMap<>();

        operatorNode = construct(IExpression);
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
    private Node construct(IExpression IExpression) throws IOException {

        if (IExpression.getType() == ExpressionType.SERIES) {
            SingleSeriesExpression singleSeriesExp = (SingleSeriesExpression) IExpression;
            SeriesReader seriesReader = generateSeriesReader(singleSeriesExp);
            Path path = singleSeriesExp.getSeriesPath();

            if (!leafCache.containsKey(path))
                leafCache.put(path, new ArrayList<>());

            // put the current reader to valueCache
            LeafNode leafNode = new LeafNode(seriesReader);
            leafCache.get(path).add(leafNode);

            return leafNode;

        } else if (IExpression.getType() == ExpressionType.OR) {
            Node leftChild = construct(((IBinaryExpression) IExpression).getLeft());
            Node rightChild = construct(((IBinaryExpression) IExpression).getRight());
            return new OrNode(leftChild, rightChild);

        } else if (IExpression.getType() == ExpressionType.AND) {
            Node leftChild = construct(((IBinaryExpression) IExpression).getLeft());
            Node rightChild = construct(((IBinaryExpression) IExpression).getRight());
            return new AndNode(leftChild, rightChild);
        }
        throw new UnSupportedDataTypeException("Unsupported ExpressionType when construct OperatorNode: " + IExpression.getType());
    }

    private SeriesReader generateSeriesReader(SingleSeriesExpression singleSeriesExp) throws IOException {
        List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(
                singleSeriesExp.getSeriesPath());
        return new SeriesReaderWithFilter(chunkLoader, chunkMetaDataList, singleSeriesExp.getFilter());
    }
}
