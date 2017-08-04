package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchIndexBuilder;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryResult;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IntervalUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.SeriesUtils;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.engine.filenode.SerializeUtil;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.IndexManager;
import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.common.DataFileInfo;
import cn.edu.thu.tsfiledb.index.common.DataFileMultiSeriesInfo;
import cn.edu.thu.tsfiledb.index.common.OverflowBufferWriteInfo;
import cn.edu.thu.tsfiledb.index.common.QueryDataSetIterator;
import cn.edu.thu.tsfiledb.index.utils.IndexFileUtils;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * The class manages the indexes of KV-match.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexManager.class);
    private static final SerializeUtil<ConcurrentHashMap<String, IndexConfig>> serializeUtil = new SerializeUtil<>();
    private static final String CONFIG_FILE_PATH = TsfileDBDescriptor.getInstance().getConfig().indexFileDir + File.separator + ".metadata";
    private static final int PARALLELISM = Runtime.getRuntime().availableProcessors() - 1;

    private static KvMatchIndexManager manager = new KvMatchIndexManager();
    private static ExecutorService executor;
    private static OverflowQueryEngine overflowQueryEngine;
    private static ConcurrentHashMap<String, IndexConfig> indexConfigStore;

    private KvMatchIndexManager() {
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

    public static KvMatchIndexManager getInstance() {
        return manager;
    }

    @Override
    public boolean build(Path columnPath, Map<String, Object> parameters) throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 0. construct index configurations
            IndexConfig indexConfig = new IndexConfig();
            indexConfig.setWindowLength((int) parameters.getOrDefault(IndexConfig.PARAM_WINDOW_LENGTH, IndexConfig.DEFAULT_WINDOW_LENGTH));
            indexConfig.setSinceTime((long) parameters.getOrDefault(IndexConfig.PARAM_SINCE_TIME, IndexConfig.DEFAULT_SINCE_TIME));

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, indexConfig.getSinceTime());

            // 2. build index for every data file.
            List<Future<Boolean>> results = new ArrayList<>(fileInfoList.size());
            for (DataFileInfo fileInfo : fileInfoList) {
                // TODO: There is a bug: the path will be modified in the query process! Have to copy a new one.
                QueryDataSet dataSet = overflowQueryEngine.getDataInTsFile(new Path(columnPath.getFullPath()), fileInfo.getFilePath());
                Future<Boolean> result = executor.submit(new KvMatchIndexBuilder(indexConfig, columnPath, dataSet, IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath())));
                results.add(result);
            }

            // 3. collect building results.
            boolean overallResult = true;
            for (Future<Boolean> result : results) {
                if (!result.get()) {
                    overallResult = false;
                }
            }

            // 4. store index configurations to memory and disk
            indexConfigStore.put(columnPath.getFullPath(), indexConfig);
            serializeUtil.serialize(indexConfigStore, CONFIG_FILE_PATH);

            return overallResult;
        } catch (FileNodeManagerException | IOException | InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } finally {
            if (token != -1) {
                try {
                    FileNodeManager.getInstance().endQuery(columnPath.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
        }
    }

    @Override
    public boolean delete(Path columnPath) throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, 0);

            // 2. delete all index files.
            for (DataFileInfo fileInfo : fileInfoList) {
                logger.info("Deleting index for '{}': [{}, {}] ({})", columnPath, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());

                File indexFile = new File(IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
                if (!indexFile.delete()) {
                    logger.warn("Can not delete obsolete index file '{}'", indexFile);
                }
                String[] subFileNames = indexFile.getParentFile().list();
                if (subFileNames == null || subFileNames.length == 0) {
                    if (!indexFile.getParentFile().delete()) {
                        logger.warn("Can not delete obsolete index directory '{}'", indexFile.getParent());
                    }
                }
            }

            // 3. delete index configurations in memory and disk
            indexConfigStore.remove(columnPath.getFullPath());
            serializeUtil.serialize(indexConfigStore, CONFIG_FILE_PATH);

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
                    FileNodeManager.getInstance().endQuery(columnPath.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
        }
    }

    @Override
    public boolean closeBuild(List<Path> columnPaths, DataFileInfo newFile) throws IndexManagerException {
        try {
            for (Path columnPath : columnPaths) {
                // 0. get configuration from store
                IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath.getFullPath(), new IndexConfig());

                // 1. build index asynchronously
                QueryDataSet dataSet = overflowQueryEngine.getDataInTsFile(columnPath, newFile.getFilePath());
                executor.submit(new KvMatchIndexBuilder(indexConfig, columnPath, dataSet, IndexFileUtils.getIndexFilePath(columnPath, newFile.getFilePath())));
            }
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

    @Override
    public boolean mergeBuild(List<DataFileMultiSeriesInfo> newFileList) throws IndexManagerException {
        try {
            // 1. get all exist index files in the file node
            Set<String> existIndexFilePathPrefixes = new HashSet<>();
            File indexFileDir = new File(IndexFileUtils.getIndexFilePathPrefix(newFileList.get(0).getFilePath())).getParentFile();
            File[] indexFiles = indexFileDir.listFiles();
            if (indexFiles != null) {
                for (File file : indexFiles) {
                    existIndexFilePathPrefixes.add(IndexFileUtils.getIndexFilePathPrefix(file));
                }
            }

            boolean overallResult = true;
            for (DataFileMultiSeriesInfo fileInfo : newFileList) {
                // 0. test whether the file is new, omit old files
                if (existIndexFilePathPrefixes.contains(IndexFileUtils.getIndexFilePathPrefix(fileInfo.getFilePath()))) {
                    continue;
                }

                // 1. build index for every series in the new file
                List<Future<Boolean>> results = new ArrayList<>(fileInfo.getColumnPaths().size());
                for (int i = 0; i < fileInfo.getColumnPaths().size(); i++) {
                    Path columnPath = fileInfo.getColumnPaths().get(i);
                    Pair<Long, Long> timeRange = fileInfo.getTimeRanges().get(i);

                    // get configuration from store
                    IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath.getFullPath(), new IndexConfig());
                    if (timeRange.right < indexConfig.getSinceTime()) continue;  // not in index range, omit

                    // build index
                    QueryDataSet dataSet = overflowQueryEngine.getDataInTsFile(columnPath, fileInfo.getFilePath());
                    Future<Boolean> result = executor.submit(new KvMatchIndexBuilder(indexConfig, columnPath, dataSet, IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath())));
                    results.add(result);
                }

                // 2. collect building results.
                for (Future<Boolean> result : results) {
                    if (!result.get()) {
                        overallResult = false;
                    }
                }
            }
            return overallResult;
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        }
    }

    @Override
    public boolean mergeSwitch(List<DataFileMultiSeriesInfo> newFileList) throws IndexManagerException {
        if (newFileList.isEmpty()) return true;  // no data file, no index file

        // get all index file path should be left
        Set<String> newIndexFilePathPrefixes = new HashSet<>(newFileList.size());
        for (DataFileMultiSeriesInfo newFile : newFileList) {
            newIndexFilePathPrefixes.add(IndexFileUtils.getIndexFilePathPrefix(newFile.getFilePath()));
        }

        // get all exist index files, and delete files not in new file list
        File indexFileDir = new File(IndexFileUtils.getIndexFilePathPrefix(newFileList.get(0).getFilePath())).getParentFile();
        File[] indexFiles = indexFileDir.listFiles();
        if (indexFiles != null) {
            for (File file : indexFiles) {
                if (!newIndexFilePathPrefixes.contains(IndexFileUtils.getIndexFilePathPrefix(file))) {
                    if (!file.delete()) {
                        logger.warn("Can not delete obsolete index file '{}'", file);
                    }
                }
            }
        }
        return true;
    }

    @Override
    public QueryDataSet query(QueryRequest queryRequest, int limitSize) throws IndexManagerException {
        Path columnPath = queryRequest.getColumnPath();
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 0. get configuration from store
            IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath.getFullPath(), new IndexConfig());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, queryRequest.getStartTime());

            // 2. fetch non-indexed ranges from overflow manager
            OverflowBufferWriteInfo overflowBufferWriteInfo = overflowQueryEngine.getDataInBufferWriteSeparateWithOverflow(columnPath, token);
            List<Pair<Long, Long>> insertOrUpdateIntervals = overflowBufferWriteInfo.getInsertOrUpdateIntervals();

            // 3. propagate query series and configurations
            KvMatchQueryRequest request = (KvMatchQueryRequest) queryRequest;
            List<Double> querySeries = getQuerySeries(request, token);
            if (querySeries.size() < 2 * indexConfig.getWindowLength() - 1) {
                throw new IllegalArgumentException(String.format("The length of query series should be greater than 2*<window_length>-1. (%s < 2*%s-1=%s)", querySeries.size(), indexConfig.getWindowLength(), (2 * indexConfig.getWindowLength() - 1)));
            }
            Pair<Long, Long> validTimeInterval = new Pair<>(Math.max(queryRequest.getStartTime(), Math.max(overflowBufferWriteInfo.getDeleteUntil() + 1, indexConfig.getSinceTime())), queryRequest.getEndTime());
            QueryConfig queryConfig = new QueryConfig(indexConfig, querySeries, request.getEpsilon(), request.getAlpha(), request.getBeta(), validTimeInterval);

            // 4. search corresponding index files of data files in the query range
            List<Future<QueryResult>> futureResults = new ArrayList<>(fileInfoList.size());
            for (int i = 0; i < fileInfoList.size(); i++) {
                DataFileInfo fileInfo = fileInfoList.get(i);
                if (fileInfo.getStartTime() > validTimeInterval.right || fileInfo.getEndTime() < validTimeInterval.left)
                    continue;  // exclude deleted, not in query range, non-indexed time intervals
                File indexFile = new File(IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
                if (indexFile.exists()) {
                    KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryConfig, columnPath, indexFile.getAbsolutePath());
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
            List<Pair<Pair<Long, Long>, Double>> answers = validateCandidatesInParallel(scanIntervals, columnPath, queryConfig, token);
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
                    FileNodeManager.getInstance().endQuery(columnPath.getDeltaObjectToString(), token);
                } catch (FileNodeManagerException e) {
                    logger.error(e.getMessage(), e.getCause());
                }
            }
        }
    }

    private List<Double> getQuerySeries(KvMatchQueryRequest request, int readToken) throws ProcessorException, PathErrorException, IOException {
        List<Pair<Long, Long>> timeInterval = new ArrayList<>(Collections.singleton(new Pair<>(request.getQueryStartTime(), request.getQueryEndTime())));
        QueryDataSetIterator queryDataSetIterator = new QueryDataSetIterator(overflowQueryEngine, request.getQueryPath(), timeInterval, readToken);
        List<Pair<Long, Double>> keyPoints = new ArrayList<>();
        while (queryDataSetIterator.hasNext()) {
            RowRecord row = queryDataSetIterator.getRowRecord();
            keyPoints.add(new Pair<>(row.getTime(), Double.parseDouble(row.getFields().get(0).getStringValue())));
        }
        if (keyPoints.isEmpty()) {
            throw new IllegalArgumentException(String.format("There is no value in the given time interval [%s, %s] for the query series %s.",  request.getQueryStartTime(), request.getQueryEndTime(), request.getQueryPath()));
        }
        return SeriesUtils.amend(keyPoints);
    }

    private List<Pair<Pair<Long, Long>, Double>> validateCandidatesInParallel(List<Pair<Long, Long>> scanIntervals, Path columnPath, QueryConfig queryConfig, int token) throws ExecutionException, InterruptedException, PathErrorException, ProcessorException, IOException {
        List<Future<List<Pair<Pair<Long, Long>, Double>>>> futureResults = new ArrayList<>(PARALLELISM);
        int intervalsPerTask = Math.max(1, (int) Math.ceil(1.0 * scanIntervals.size() / PARALLELISM)), i = 0;
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
        startTime.setDeltaObjectType("Start Time");  // useless names
        DynamicOneColumnData endTime = new DynamicOneColumnData(TSDataType.INT64, true);
        endTime.setDeltaObjectType("End Time");
        DynamicOneColumnData distance = new DynamicOneColumnData(TSDataType.DOUBLE, true);
        distance.setDeltaObjectType("Distance");
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
}
