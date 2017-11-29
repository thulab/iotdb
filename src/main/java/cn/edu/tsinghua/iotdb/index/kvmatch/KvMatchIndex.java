package cn.edu.tsinghua.iotdb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.iotdb.KvMatchIndexBuilder;
import cn.edu.fudan.dsm.kvmatch.iotdb.KvMatchQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.iotdb.common.IndexConfig;
import cn.edu.fudan.dsm.kvmatch.iotdb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.iotdb.common.QueryResult;
import cn.edu.fudan.dsm.kvmatch.iotdb.utils.IntervalUtils;
import cn.edu.fudan.dsm.kvmatch.iotdb.utils.SeriesUtils;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.filenode.SerializeUtil;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.index.IoTIndex;
import cn.edu.tsinghua.iotdb.index.common.DataFileInfo;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.index.common.OverflowBufferWriteInfo;
import cn.edu.tsinghua.iotdb.index.common.QueryDataSetIterator;
import cn.edu.tsinghua.iotdb.index.utils.IndexFileUtils;
import cn.edu.tsinghua.iotdb.metadata.MManager;
import cn.edu.tsinghua.iotdb.query.engine.OverflowQueryEngine;
import cn.edu.tsinghua.iotdb.query.engine.ReadCachePrefix;
import cn.edu.tsinghua.iotdb.query.reader.RecordReader;
import cn.edu.tsinghua.iotdb.query.management.RecordReaderFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.LongInterval;
import cn.edu.tsinghua.tsfile.timeseries.filter.verifier.FilterVerifier;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * kv-match实现的单例索引
 */
public class KvMatchIndex  implements IoTIndex {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndex.class);
    private static final SerializeUtil<ConcurrentHashMap<String, IndexConfig>> serializeUtil = new SerializeUtil<>();
    private static final String CONFIG_FILE_PATH = TsfileDBDescriptor.getInstance().getConfig().indexFileDir + File.separator + ".metadata";
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() - 1;

    private static final String dataDirPath = TsfileDBDescriptor.getInstance().getConfig().fileNodeDir;

    private static OverflowQueryEngine overflowQueryEngine;
    private static ExecutorService executor;
    private static ConcurrentHashMap<String, IndexConfig> indexConfigStore;

    private KvMatchIndex() {
        executor = Executors.newFixedThreadPool(PARALLELISM);
        overflowQueryEngine = new OverflowQueryEngine();
        try {
            File file = new File(CONFIG_FILE_PATH);
            FileUtils.forceMkdirParent(file);
            indexConfigStore = serializeUtil.deserialize(CONFIG_FILE_PATH).orElse(new ConcurrentHashMap<>());
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    private static class KvMatchIndexHolder {
        static final KvMatchIndex INSTANCE = new KvMatchIndex();
    }

    /**
     * only be used for {@linkplain cn.edu.tsinghua.iotdb.index.IndexManager IndexManager}
     * @return
     */
    public static KvMatchIndex getInstance() { return KvMatchIndexHolder.INSTANCE; }

    /**
     * 初始化索引，digest要初始化一些内存中的节点，KVIndex不做任何事
     */
    @Override
    public void init() {

    }

    /**
     * 为指定文件构建索引。注意，参数中给出了现有的文件列表，索引自己可以决定是
     * "完全更新"，还是增量式更新并删除不再存在的文件（KVIndex的做法）。
     * 这一过程可以是异步的，对于异步的构建索引，立即返回然后开始建立索引，如果此时来了查询，简单的做法是直接当做没有索引来处理
     *
     * @param path       待修改的索引的path
     * @param parameters 其他参数，可选
     * @return whether the operation is successful
     * @throws IndexManagerException
     */
    @Override
    public boolean build(Path path, List<DataFileInfo> fileList, Map<String, Object> parameters)
            throws IndexManagerException {
        int token = -1;
        List<String> indexFls = new ArrayList<>();
        Boolean overall = true;
        try {
            // 0. construct index configurations
            IndexConfig indexConfig = new IndexConfig();
            if (parameters == null) {
                indexConfig = indexConfigStore.getOrDefault(path.getFullPath(), new IndexConfig());
            }
            else {
                indexConfig.setWindowLength((int) parameters.getOrDefault(IndexConfig.PARAM_WINDOW_LENGTH, IndexConfig.DEFAULT_WINDOW_LENGTH));
                indexConfig.setSinceTime((long) parameters.getOrDefault(IndexConfig.PARAM_SINCE_TIME, IndexConfig.DEFAULT_SINCE_TIME));
            }

            long startTime = indexConfig.getSinceTime();

            // 1. build index for every data file
            if (fileList == null || fileList.isEmpty()) {
                token = FileNodeManager.getInstance().beginQuery(path.getDeltaObjectToString());
                fileList = FileNodeManager.getInstance().indexBuildQuery(path, indexConfig.getSinceTime(), -1);
            }

            if (fileList.isEmpty()) {
                if (overall && parameters != null) {
                    indexConfigStore.put(path.getFullPath(), indexConfig);
                    serializeUtil.serialize(indexConfigStore, CONFIG_FILE_PATH);
                }
                return true;
            }

            Set<String> existIndexFilePaths = new HashSet<>();
            File indexFileDir = new File(IndexFileUtils.getIndexFilePathPrefix(fileList.get(0).getFilePath())).getParentFile();
            File[] indexFiles = indexFileDir.listFiles();
            if (indexFiles != null) {
                for (File file : indexFiles) {
                    existIndexFilePaths.add(file.getAbsolutePath());
                }
            }

            for (DataFileInfo fileInfo : fileList) {
                String indexFile = IndexFileUtils.getIndexFilePath(path, fileInfo.getFilePath());

                // 0. test whether the file is new, omit old files
                if (existIndexFilePaths.contains(indexFile)) {
                    continue;
                }

                if (startTime > fileInfo.getEndTime()) {
                    continue;
                }

                File buildFile = new File(indexFile + ".building");
                if (buildFile.delete()) {
                    logger.warn("{} delete failed".format(buildFile.getAbsolutePath()));
                }

                QueryDataSet dataSet = getDataInTsFile(path, fileInfo.getFilePath());
                Future<Boolean> result = executor.submit(new KvMatchIndexBuilder(indexConfig, path, dataSet, indexFile));
                indexFls.add(indexFile);
                Boolean rs = result.get();
                if (!rs) {
                    overall = false;
                    break;
                }
            }

            if (overall && parameters != null) {
                indexConfigStore.put(path.getFullPath(), indexConfig);
                serializeUtil.serialize(indexConfigStore, CONFIG_FILE_PATH);
            }

            return overall;
        } catch (FileNodeManagerException | IOException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } finally {
            if (token != -1) {
                try {
                    FileNodeManager.getInstance().endQuery(path.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
            if (!overall) {
                for (String fl : indexFls) {
                    File indexFl = new File(fl);
                    if (!indexFl.delete()) {
                        logger.warn("Can not delete obsolete index file '{}' when build failed", indexFl);
                    }
                }
            }
        }
    }

    /**
     * 与上面唯一的不同在于，只新加了一个文件。两个函数区别开是因为，上面的函数多用于完全新建或者merge的时候，
     * 而这个发生在关掉一个fileNode形成新文件的时候。
     * 这一过程可以是异步的，对于异步的构建索引，立即返回然后开始建立索引，如果此时来了查询，简单的做法是直接当做没有索引来处理
     *
     * @param path       待修改的索引的path
     * @param newFile    新加的一个文件；
     * @param parameters 其他参数，可选
     * @return
     * @throws IndexManagerException
     */
    @Override
    public boolean build(Path path, DataFileInfo newFile, Map<String, Object> parameters)
            throws IndexManagerException {
        try {
            // 0. construct index configurations
            IndexConfig indexConfig = new IndexConfig();
            if (parameters == null) {
                indexConfig = indexConfigStore.getOrDefault(path.getFullPath(), new IndexConfig());
            }
            else {
                indexConfig.setWindowLength((int) parameters.getOrDefault(IndexConfig.PARAM_WINDOW_LENGTH, IndexConfig.DEFAULT_WINDOW_LENGTH));
                indexConfig.setSinceTime((long) parameters.getOrDefault(IndexConfig.PARAM_SINCE_TIME, IndexConfig.DEFAULT_SINCE_TIME));
            }

            long startTime = indexConfig.getSinceTime();

            if (startTime > newFile.getEndTime()) {
                return true;
            }

            String indexFile = IndexFileUtils.getIndexFilePath(path, newFile.getFilePath());
            File indexFl = new File(indexFile);
            if (indexFl.exists()) {
                return true;
            }

            File buildFl = new File(indexFile + ".building");
            if (buildFl.delete()) {
                logger.warn("{} delete failed".format(buildFl.getAbsolutePath()));
            }

            // 1. build index asynchronously
            QueryDataSet dataSet = getDataInTsFile(path, newFile.getFilePath());
            Future<Boolean> result = executor.submit(new KvMatchIndexBuilder(indexConfig, path, dataSet, indexFile));
            result.get();
//            KvMatchIndexBuilder rs = new KvMatchIndexBuilder(indexConfig, path, dataSet, IndexFileUtils.getIndexFilePath(path, newFile.getFilePath()));
//            Boolean rr = rs.call();
            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        }
    }

    /**
     * Given the new file list after merge, delete all index files which are not in the list,
     * and switch to the new index files along with the new data files.
     * Call this method after the merge operation has completed. Block index read and write during this process.
     *
     * @param newFileList the data files leaves after the merge operation, the column paths in the file list need to build index, some one may has no data in some data file
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    @Override
    public boolean mergeSwitch(Path path, List<DataFileInfo> newFileList) throws IndexManagerException {
        Set<String> newIndexFilePathPrefixes = new HashSet<>();
        for (DataFileInfo fileInfo : newFileList) {
            newIndexFilePathPrefixes.add(IndexFileUtils.getIndexFilePathPrefix(fileInfo.getFilePath()));
        }
        File indexFileDir = new File(IndexFileUtils.getIndexFilePathPrefix(newFileList.get(0).getFilePath())).getParentFile();
        String suffix = IndexFileUtils.getIndexFilePathSuffix(IndexFileUtils.getIndexFilePath(path, newFileList.get(0).getFilePath()));
        File[] indexFiles = indexFileDir.listFiles();
        if (indexFiles != null) {
            for (File file : indexFiles) {
                if (suffix.equals(IndexFileUtils.getIndexFilePathSuffix(file)) && !newIndexFilePathPrefixes.contains(IndexFileUtils.getIndexFilePathPrefix(file))) {
                    if (!file.delete()) {
                        logger.warn("Can not delete obsolete index file '{}'", file);
                    }
                }
            }
        }
        return true;
    }

    /**
     * 末位添加，只用于实时索引
     * @param path
     * @param timestamp
     * @param value
     */
    @Override
    public void append(Path path, long timestamp, String value) {
    }

    /**
     * 单点更新，只用于实时索引
     * @param path
     * @param timestamp
     * @param value
     */
    @Override
    public void update(Path path, long timestamp, String value) {
    }

    /**
     * 区间段更新，只用于实时索引
     * @param path
     * @param starttime
     * @param endtime
     * @param value
     */
    @Override
    public void update(Path path, long starttime, long endtime, String value) {
    }

    /**
     * 某个时间点前的删除，只用于实时索引
     * @param path
     * @param timestamp
     */
    @Override
    public void delete(Path path, long timestamp) {
    }

    /**
     * 将整个索引关闭，如果在内存中有一些信息，则要序列化到磁盘上。注意，这一步需要是同步的。
     *
     * @return 是否正确完成
     * @throws IndexManagerException
     */
    @Override
    public boolean close() throws IndexManagerException {
        return true;
    }

    /**
     * 彻底删除一个path的索引
     *
     * @param path the column path
     * @return whether the operation is successful
     * @throws IndexManagerException
     */
    @Override
    public boolean drop(Path path) throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(path.getDeltaObjectToString());

            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(path, 0, -1);

            for (DataFileInfo fileInfo : fileInfoList) {
                logger.info("Deleting index for '{}': [{}, {}] ({})", path, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());

                File indexFile = new File(IndexFileUtils.getIndexFilePath(path, fileInfo.getFilePath()));
                if (!indexFile.delete()) {
                    logger.warn("Can not delete obsolete index file '{}'", indexFile);
                }
                String[] subFilenames = indexFile.getParentFile().list();
                if (subFilenames == null || subFilenames.length == 0) {
                    if (!indexFile.getParentFile().delete()) {
                        logger.warn("Can not delete obsolete index directory '{}'", indexFile.getParent());
                    }
                }

                indexConfigStore.remove(path.getFullPath());
                serializeUtil.serialize(indexConfigStore, CONFIG_FILE_PATH);

            }
            return true;
        } catch (FileNodeManagerException | IOException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } finally {
            if (token != -1) {
                try {
                    FileNodeManager.getInstance().endQuery(path.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
        }
    }

    /**
     * 输入查询请求和未被更改过的区间，以及单次返回的数量，查询。
     * 这个查询针对于功能式索引，返回的结果就是要显示给用户的结果
     * 插件式索引由于自带新功能，所以无论是否见好索引，都应该返回预期的结果（可以是没有建好索引的部分就暴力搜索，也可以返回"无可奉告"）
     *
     * @param path       the path to be queried
     * @param parameters       the query request with all parameters
     * @param nonUpdateIntervals the query request with all parameters
     * @param limitSize          the limitation of number of answers
     * @return the query response
     */
    @Override
    public Object query(Path path, List<Object> parameters, List<Pair<Long, Long>> nonUpdateIntervals, int limitSize)
            throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(path.getDeltaObjectToString());

            // 0. get configuration from store
            IndexConfig indexConfig = indexConfigStore.getOrDefault(path.getFullPath(), new IndexConfig());

            // 1. get all parameters
            long startTime = (long)(parameters.get(0));
            long endTime = (long)(parameters.get(1));
            if (endTime == -1) {
                endTime = Long.MAX_VALUE;
            }
            Path queryPath = (Path)(parameters.get(2));
            long queryStartTime = (long)(parameters.get(3));
            long queryEndTime = (long)(parameters.get(4));
            if (queryEndTime == -1) {
                queryEndTime = Long.MAX_VALUE;
            }
            double epsilon = (double)(parameters.get(5));
            double alpha = (double)(parameters.get(6));
            double beta = (double)(parameters.get(7));

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(path, startTime, endTime);

            // 2. fetch non-indexed ranges from overflow manager
            OverflowBufferWriteInfo overflowBufferWriteInfo = getDataInBufferWriteSeparateWithOverflow(path, token);
            List<Pair<Long, Long>> insertOrUpdateIntervals = overflowBufferWriteInfo.getInsertOrUpdateIntervals();

            // 3. propagate query series and configurations
            List<Double> querySeries = getQuerySeries(queryPath, queryStartTime, queryEndTime, token);
            if (querySeries.size() < 2 * indexConfig.getWindowLength() - 1) {
                throw new IllegalArgumentException(String.format("The length of query series should be greater than 2*<window_length>-1. (%s < 2*%s-1=%s)", querySeries.size(), indexConfig.getWindowLength(), (2 * indexConfig.getWindowLength() - 1)));
            }
            Pair<Long, Long> validTimeInterval = new Pair<>(Math.max(startTime, Math.max(overflowBufferWriteInfo.getDeleteUntil() + 1, indexConfig.getSinceTime())), endTime);
            QueryConfig queryConfig = new QueryConfig(indexConfig, querySeries, epsilon, alpha, beta, validTimeInterval);

            // 4. search corresponding index files of data files in the query range
            List<Future<QueryResult>> futureResults = new ArrayList<>(fileInfoList.size());
            for (int i = 0; i < fileInfoList.size(); i++) {
                DataFileInfo fileInfo = fileInfoList.get(i);
                if (fileInfo.getStartTime() > validTimeInterval.right || fileInfo.getEndTime() < validTimeInterval.left)
                    continue;  // exclude deleted, not in query range, non-indexed time intervals
                File indexFile = new File(IndexFileUtils.getIndexFilePath(path, fileInfo.getFilePath()));
                if (indexFile.exists()) {
                    KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryConfig, path, indexFile.getAbsolutePath());
                    Future<QueryResult> result = executor.submit(queryExecutor);
                    futureResults.add(result);
                } else {  // the index of this file has not been built, this will not happen in normal circumstance (likely to happen between close operation and index building of new file finished)
                    insertOrUpdateIntervals.add(fileInfo.getTimeInterval());
                }
                if (i > 0) {  // add time intervals between file
                    insertOrUpdateIntervals.add(new Pair<>(fileInfo.getStartTime(), fileInfo.getStartTime()));
                }
            }

            // 5. collect query results
            QueryResult overallResult = new QueryResult();
            for (Future<QueryResult> result : futureResults) {
                if (result.get() != null) {
                    overallResult.addCandidateRanges(result.get().getCandidateRanges());
                }
            }

            // 6. merge the candidate ranges and non-indexed ranges to produce candidate ranges
            insertOrUpdateIntervals = IntervalUtils.extendBoth(insertOrUpdateIntervals, querySeries.size());
            insertOrUpdateIntervals = IntervalUtils.sortAndMergePair(insertOrUpdateIntervals);
            overallResult.setCandidateRanges(IntervalUtils.sortAndMergePair(overallResult.getCandidateRanges()));
            overallResult.setCandidateRanges(IntervalUtils.union(overallResult.getCandidateRanges(), insertOrUpdateIntervals));
            overallResult.setCandidateRanges(IntervalUtils.excludeNotIn(overallResult.getCandidateRanges(), validTimeInterval));
            logger.trace("Candidates: {}", overallResult.getCandidateRanges());

            // 7. scan the data in candidate ranges to find out actual answers and sort them by distances
            List<Pair<Long, Long>> scanIntervals = IntervalUtils.extendAndMerge(overallResult.getCandidateRanges(), querySeries.size());
            List<Pair<Pair<Long, Long>, Double>> answers = validateCandidatesInParallel(scanIntervals, path, queryConfig, token);
            answers.sort(Comparator.comparingDouble(o -> o.right));
            logger.trace("Answers: {}", answers);

            return constructQueryDataSet(answers, limitSize);
        } catch (FileNodeManagerException | InterruptedException | ExecutionException | ProcessorException | IOException | PathErrorException | IllegalArgumentException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } finally {
            if (token != -1) {
                try {
                    FileNodeManager.getInstance().endQuery(path.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
        }
    }

    private List<Double> getQuerySeries(Path path, long startTime, long endTime, int readToken) throws ProcessorException, PathErrorException, IOException {
        List<Pair<Long, Long>> timeInterval = new ArrayList<>(Collections.singleton(new Pair<>(startTime, endTime)));
        QueryDataSetIterator queryDataSetIterator = new QueryDataSetIterator(overflowQueryEngine, path, timeInterval, readToken);
        List<Pair<Long, Double>> keyPoints = new ArrayList<>();
        while (queryDataSetIterator.hasNext()) {
            RowRecord row = queryDataSetIterator.getRowRecord();
            keyPoints.add(new Pair<>(row.getTime(), SeriesUtils.getValue(row.getFields().get(0))));
        }
        String prefix = ReadCachePrefix.addQueryPrefix(0);
        RecordReaderFactory.getInstance().removeRecordReader(prefix + path.getDeltaObjectToString(), path.getMeasurementToString());
        if (keyPoints.isEmpty()) {
            throw new IllegalArgumentException(String.format("There is no value in the given time interval [%s, %s] for the query series %s.", startTime, endTime, path));
        }
        return SeriesUtils.amend(keyPoints);
    }

    private List<Pair<Pair<Long, Long>, Double>> validateCandidatesInParallel(List<Pair<Long, Long>> scanIntervals, Path columnPath, QueryConfig queryConfig, int token) throws ExecutionException, InterruptedException, PathErrorException, ProcessorException, IOException {
        List<Future<List<Pair<Pair<Long, Long>, Double>>>> futureResults = new ArrayList<>(PARALLELISM);
        int intervalsPerTask = Math.min(Math.max(1, (int) Math.ceil(1.0 * scanIntervals.size() / PARALLELISM)), (new LongInterval()).v.length), i = 0;  // TODO: change LongInterval.arrayMaxn to public static field
        while (i < scanIntervals.size()) {
            List<Pair<Long, Long>> partialScanIntervals = scanIntervals.subList(i, Math.min(scanIntervals.size(), i + intervalsPerTask));
            i += intervalsPerTask;
            // schedule validating task
            KvMatchCandidateValidator validator = new KvMatchCandidateValidator(columnPath, partialScanIntervals, queryConfig, token);
            Future<List<Pair<Pair<Long, Long>, Double>>> result = executor.submit(validator);
            futureResults.add(result);
        }
        // collect results
        List<Pair<Pair<Long, Long>, Double>> overallResult = new ArrayList<>();
        for (Future<List<Pair<Pair<Long, Long>, Double>>> result : futureResults) {
            if (result.get() != null) {
                overallResult.addAll(result.get());
            }
        }
        return overallResult;
    }

    private QueryDataSet constructQueryDataSet(List<Pair<Pair<Long, Long>, Double>> answers, int limitSize) throws IOException, ProcessorException {
        QueryDataSet dataSet = new QueryDataSet();
        DynamicOneColumnData startTime = new DynamicOneColumnData(TSDataType.INT64, true);
        DynamicOneColumnData endTime = new DynamicOneColumnData(TSDataType.INT64, true);
        DynamicOneColumnData distance = new DynamicOneColumnData(TSDataType.DOUBLE, true);
        for (int i = 0; i < Math.min(limitSize, answers.size()); i++) {
            Pair<Pair<Long, Long>, Double> answer = answers.get(i);
            startTime.putTime(i);
            startTime.putLong(answer.left.left);
            endTime.putTime(i);
            endTime.putLong(answer.left.right);
            distance.putTime(i);
            distance.putDouble(answer.right);
        }
        dataSet.mapRet.put("Start.Time", startTime);  // useless names
        dataSet.mapRet.put("End.Time", endTime);
        dataSet.mapRet.put("Distance.", distance);
        return dataSet;
    }

    /**
     * kv-index, get the OverflowData and BufferWriteData separately only in memory.
     * No use to release read lock, because this method will not use alone.
     *
     * @param path kv-index path
     * @return
     * @throws PathErrorException
     * @throws IOException
     * @throws ProcessorException
     */
    public OverflowBufferWriteInfo getDataInBufferWriteSeparateWithOverflow(Path path, int readToken) throws PathErrorException, IOException, ProcessorException {
        String deltaObjectUID = path.getDeltaObjectToString();
        String measurementUID = path.getMeasurementToString();
        String recordReaderPrefix = ReadCachePrefix.addQueryPrefix(0);

        RecordReader recordReader = RecordReaderFactory.getInstance().
                getRecordReader(deltaObjectUID, measurementUID, null, null, null, readToken, recordReaderPrefix);

        long bufferWriteBeginTime = Long.MAX_VALUE;
        if (recordReader.bufferWritePageList != null && recordReader.bufferWritePageList.size() > 0) {
            PageReader pageReader = new PageReader(recordReader.bufferWritePageList.get(0), recordReader.compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();
            bufferWriteBeginTime = pageHeader.data_page_header.min_timestamp;
        } else if (recordReader.insertPageInMemory != null && recordReader.insertPageInMemory.timeLength > 0) {
            bufferWriteBeginTime = recordReader.insertPageInMemory.getTime(0);
        }

        DynamicOneColumnData insert = (DynamicOneColumnData) recordReader.overflowInfo.get(0);
        DynamicOneColumnData update = (DynamicOneColumnData) recordReader.overflowInfo.get(1);
        SingleSeriesFilterExpression deleteFilter = (SingleSeriesFilterExpression) recordReader.overflowInfo.get(3);
        long maxDeleteTime = 0;
        if (deleteFilter != null) {
            LongInterval interval = (LongInterval) FilterVerifier.create(TSDataType.INT64).getInterval(deleteFilter);
            if (interval.count > 0) {
                if (interval.flag[0] && interval.v[0] > 0) {
                    maxDeleteTime = interval.v[0] - 1;
                } else {
                    maxDeleteTime = interval.v[0];
                }
            }
        }

        RecordReaderFactory.getInstance().removeRecordReader(recordReaderPrefix + deltaObjectUID, measurementUID);
        return new OverflowBufferWriteInfo(insert, update, maxDeleteTime < 0 ? 0L : maxDeleteTime, bufferWriteBeginTime);
    }

    /**
     * get the data only in file
     */
    public QueryDataSet getDataInTsFile(Path path, String filePath) throws IOException {
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(filePath);
        TsFile readTsFile = new TsFile(input);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(path);
        return readTsFile.query(paths, null, null);
    }
}
