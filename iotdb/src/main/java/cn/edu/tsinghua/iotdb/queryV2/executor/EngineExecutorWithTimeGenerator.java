package cn.edu.tsinghua.iotdb.queryV2.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.queryV2.dataset.EngineDataSetWithTimeGenerator;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.iotdb.queryV2.reader.merge.EngineSeriesReaderByTimeStamp;
import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PriorityMergeReader;
import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PriorityMergeReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.reader.sequence.SequenceDataReader;
import cn.edu.tsinghua.iotdb.read.IReader;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceManager;
import cn.edu.tsinghua.iotdb.read.reader.QueryByTimestampsReader;
import cn.edu.tsinghua.iotdb.queryV2.timegenerator.EngineTimeGenerator;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.QueryExpression;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * IoTDB query executor with filter
 */
public class EngineExecutorWithTimeGenerator {

    public EngineExecutorWithTimeGenerator() {
    }

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(queryExpression.getExpression());

        List<EngineSeriesReaderByTimeStamp> readersOfSelectedSeries = new ArrayList<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new EngineDataSetWithTimeGenerator(timestampGenerator, readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(List<EngineSeriesReaderByTimeStamp> readersOfSelectedSeries, List<Path> selectedSeries)
            throws IOException, FileNodeManagerException {


        for (Path path : selectedSeries) {

            QueryDataSource queryDataSource = QueryDataSourceManager.getQueryDataSource(path);
            PriorityMergeReaderByTimestamp mergeReaderByTimestamp = new PriorityMergeReaderByTimestamp();

            // reader for sequence data
            SequenceDataReader tsFilesReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), null);
            mergeReaderByTimestamp.addReaderWithPriority(tsFilesReader, 1);

            // reader for unSequence data
            PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance().
                    createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), null);
            mergeReaderByTimestamp.addReaderWithPriority(unSeqMergeReader, 2);

            readersOfSelectedSeries.add(mergeReaderByTimestamp);
        }
    }

}
