package cn.edu.tsinghua.iotdb.query.management;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.query.reader.ReaderType;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;

import java.io.IOException;

/**
 * To avoid create RecordReader frequently,<br>
 * RecordReaderFactory could create a RecordReader using cache.
 *
 * @author Jinrui Zhang
 */
public class RecordReaderFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordReaderFactory.class);
    private static RecordReaderFactory instance = new RecordReaderFactory();

    private FileNodeManager fileNodeManager;
    private ReadLockManager readLockManager;

    private RecordReaderFactory() {
        fileNodeManager = FileNodeManager.getInstance();
        readLockManager = ReadLockManager.getInstance();
    }

    public RecordReader getRecordReader(String deltaObjectUID, String measurementID,
                                        SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                        Integer readLock, String prefix, ReaderType readerType) throws ProcessorException, PathErrorException {
        int token = 0;
        if (readLock == null) {
            token = readLockManager.lock(deltaObjectUID);
        } else {
            token = readLock;
        }
        String cacheDeltaKey = prefix + deltaObjectUID;
        if (readLockManager.recordReaderCache.containsRecordReader(cacheDeltaKey, measurementID)) {
            //return readLockManager.recordReaderCache.get(cacheDeltaKey, measurementID);
            return null;
        } else {
            QueryDataSource queryDataSource;
            try {
                queryDataSource = fileNodeManager.query(deltaObjectUID, measurementID, timeFilter, null, valueFilter);
            } catch (FileNodeManagerException e) {
                throw new ProcessorException(e.getMessage());
            }
            RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, queryDataSource, readerType);
            readLockManager.recordReaderCache.put(cacheDeltaKey, measurementID, null);
            return recordReader;
        }
    }

    public RecordReader getRecordReaderV2(String deltaObjectUID, String measurementID,
                                        SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression valueFilter,
                                        Integer readLock, String prefix, ReaderType readerType) throws ProcessorException, PathErrorException {
        int token = 0;
        if (readLock == null) {
            token = readLockManager.lock(deltaObjectUID);
        } else {
            token = readLock;
        }
        String cacheDeltaKey = prefix + deltaObjectUID;
        if (readLockManager.recordReaderCache.containsRecordReader(cacheDeltaKey, measurementID)) {
            //return readLockManager.recordReaderCache.get(cacheDeltaKey, measurementID);
            return null;
        } else {
            QueryDataSource queryDataSource;
            try {
                queryDataSource = fileNodeManager.query(deltaObjectUID, measurementID, timeFilter, null, valueFilter);
            } catch (FileNodeManagerException e) {
                throw new ProcessorException(e.getMessage());
            }
            RecordReader recordReader = createANewRecordReader(deltaObjectUID, measurementID, queryDataSource, readerType);
            readLockManager.recordReaderCache.put(cacheDeltaKey, measurementID, null);
            return recordReader;
        }
    }

    private RecordReader createANewRecordReader(String deltaObjectUID, String measurementID,
                                                QueryDataSource queryDataSource, ReaderType readerType) throws PathErrorException {
        RecordReader recordReader = null;

//        List<IntervalFileNode> fileNodes = queryDataSource.getBufferwriteDataInFiles();
//        boolean hasUnEnvelopedFile;
//        if (fileNodes.size() > 0 && !fileNodes.get(fileNodes.size() - 1).isClosed()) {
//            hasUnEnvelopedFile = true;
//        } else {
//            hasUnEnvelopedFile = false;
//        }
//        List<String> filePathList = new ArrayList<>();
//        for (int i = 0; i < fileNodes.size() - 1; i++) {
//            IntervalFileNode fileNode = fileNodes.get(i);
//            filePathList.add(fileNode.getFilePath());
//        }
//        if (hasUnEnvelopedFile) {
//            String unsealedFilePath = fileNodes.get(fileNodes.size() - 1).getFilePath();
//
//            // if currentPage is null, both currentPage and pageList must both are null
//            if (queryDataSource.getCurrentPage() == null) {
//                switch (readerType) {
//                    case QUERY:
//                        recordReader = new QueryRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID, null, null, null,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                    case AGGREGATE:
//                        recordReader = new AggregateRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID,null, null, null,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                    case FILL:
//                        recordReader = new FillRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID, null, null, null,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                }
//            } else {
//                switch (readerType) {
//                    case QUERY:
//                        recordReader = new QueryRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID, queryDataSource.getCurrentPage(),
//                                queryDataSource.getPageList().left, queryDataSource.getPageList().right, queryDataSource.getAllOverflowData());
//                        break;
//                    case AGGREGATE:
//                        recordReader = new AggregateRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID, queryDataSource.getCurrentPage(),
//                                queryDataSource.getPageList().left, queryDataSource.getPageList().right, queryDataSource.getAllOverflowData());
//                        break;
//                    case FILL:
//                        recordReader = new FillRecordReader(filePathList, unsealedFilePath, queryDataSource.getBufferwriteDataInDisk(),
//                                deltaObjectUID, measurementID, queryDataSource.getCurrentPage(),
//                                queryDataSource.getPageList().left, queryDataSource.getPageList().right, queryDataSource.getAllOverflowData());
//                        break;
//                }
//            }
//        } else {
//            if (fileNodes.size() > 0) {
//                filePathList.add(fileNodes.get(fileNodes.size() - 1).getFilePath());
//            }
//            if (queryDataSource.getCurrentPage() == null) {
//                switch (readerType) {
//                    case QUERY:
//                        recordReader = new QueryRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), null, null, queryDataSource.getAllOverflowData());
//                        break;
//                    case AGGREGATE:
//                        recordReader = new AggregateRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), null, null, queryDataSource.getAllOverflowData());
//                        break;
//                    case FILL:
//                        recordReader = new FillRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), null, null, queryDataSource.getAllOverflowData());
//                        break;
//                }
//            } else {
//                switch (readerType) {
//                    case QUERY:
//                        recordReader = new QueryRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), queryDataSource.getPageList().left, queryDataSource.getPageList().right,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                    case AGGREGATE:
//                        recordReader = new AggregateRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), queryDataSource.getPageList().left, queryDataSource.getPageList().right,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                    case FILL:
//                        recordReader = new FillRecordReader(filePathList, deltaObjectUID, measurementID,
//                                queryDataSource.getCurrentPage(), queryDataSource.getPageList().left, queryDataSource.getPageList().right,
//                                queryDataSource.getAllOverflowData());
//                        break;
//                }
//            }
//        }

        return recordReader;

    }

    public static RecordReaderFactory getInstance() {
        return instance;
    }

    // TODO this method is only used in test case and KV-match index
    public void removeRecordReader(String deltaObjectId, String measurementId) throws IOException {
        if (readLockManager.recordReaderCache.containsRecordReader(deltaObjectId, measurementId)) {
            // close the RecordReader read stream.
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).closeFileStream();
            readLockManager.recordReaderCache.get(deltaObjectId, measurementId).closeFileStreamForOneRequest();
            readLockManager.recordReaderCache.remove(deltaObjectId, measurementId);
        }
    }
}
