//package cn.edu.tsinghua.iotdb.read.reader;
//
//import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
//import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeReaderByTimestamp;
//import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PrioritySeriesReader;
//import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PrioritySeriesReaderByTimestamp;
//import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReaderByTimeStamp;
//import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
//import cn.edu.tsinghua.iotdb.read.ISeriesReader;
//import cn.edu.tsinghua.iotdb.utils.TimeValuePair;
//import cn.edu.tsinghua.iotdb.utils.TsPrimitiveType;
//
//import java.io.IOException;
//
///**
// * A reader that can get the corresponding value of the specified time point.
// * It has considered sequence insert data, overflow data.
// *
// * TODO: update and delete operation.
// * */
//public class QueryByTimestampsReader implements ISeriesReader {
//
//    //private SeriesWithOverflowOpReader seriesWithOverflowOpReader;
//
//    public QueryByTimestampsReader(QueryDataSource queryDataSource) throws IOException {
//
//        //sequence insert data
//        SequenceInsertDataByTimeStampReader tsFilesReader = new SequenceInsertDataByTimeStampReader(queryDataSource.getSeriesDataSource());
//        PrioritySeriesReaderByTimestamp tsFilesReaderWithPriority = new PrioritySeriesReaderByTimestamp(
//                tsFilesReader, new PrioritySeriesReader.Priority(1));
//
//        //overflow insert data
//        OverflowInsertDataReaderByTimeStamp overflowInsertDataReader = SeriesReaderFactory.getInstance().
//                createSeriesReaderForOverflowInsertByTimestamp(queryDataSource.getOverflowSeriesDataSource());
//        PrioritySeriesReaderByTimestamp overflowInsertDataReaderWithPriority = new PrioritySeriesReaderByTimestamp(
//                overflowInsertDataReader, new PrioritySeriesReader.Priority(2));
//
//        //operation of update and delete
//        OverflowOperationReader overflowOperationReader =
//                queryDataSource.getOverflowSeriesDataSource().getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReaderNewInstance();
//
//        PriorityMergeReaderByTimestamp insertDataReader =
//                new PriorityMergeReaderByTimestamp(tsFilesReaderWithPriority, overflowInsertDataReaderWithPriority);
//
//        seriesWithOverflowOpReader =
//                new SeriesWithOverflowOpReader(insertDataReader, overflowOperationReader);
//    }
//
//
//    @Override
//    public boolean hasNext() throws IOException {
//        return seriesWithOverflowOpReader.hasNext();
//    }
//
//    @Override
//    public TimeValuePair next() throws IOException {
//        return seriesWithOverflowOpReader.next();
//    }
//
//    @Override
//    public void skipCurrentTimeValuePair() throws IOException {
//        next();
//    }
//
//    @Override
//    public void close() throws IOException {
//        seriesWithOverflowOpReader.close();
//    }
//
//
//    @Override
//    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
//        return seriesWithOverflowOpReader.getValueInTimestamp(timestamp);
//    }
//}
