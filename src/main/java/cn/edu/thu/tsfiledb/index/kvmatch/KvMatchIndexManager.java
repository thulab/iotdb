package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchIndexBuilder;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryResult;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IntervalUtils;
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
import cn.edu.thu.tsfiledb.index.*;
import cn.edu.thu.tsfiledb.index.common.DataFileInfo;
import cn.edu.thu.tsfiledb.index.common.DataFileMultiSeriesInfo;
import cn.edu.thu.tsfiledb.index.common.OverflowBufferWrite;
import cn.edu.thu.tsfiledb.index.utils.IndexFileUtils;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The class manage the indexes of KV-match.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexManager.class);

    private static KvMatchIndexManager manager = null;

    private ExecutorService executor;

    private OverflowQueryEngine overflowQueryEngine;

    private static final String CONFIG_FILE_PATH = TsfileDBDescriptor.getInstance().getConfig().indexFileDir + File.separator + ".metadata";
    private Map<String, IndexConfig> indexConfigStore;
    private SerializeUtil<Map<String, IndexConfig>> serializeUtil = new SerializeUtil<>();

    private KvMatchIndexManager() {
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 1);
        overflowQueryEngine = new OverflowQueryEngine();
        try {
            File file = new File(CONFIG_FILE_PATH);
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IOException("Can not create directory " + file.getParent());
                }
            }
            indexConfigStore = serializeUtil.deserialize(CONFIG_FILE_PATH).orElse(new HashMap<>());
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    public static KvMatchIndexManager getInstance() {
        if (manager == null) {
            manager = new KvMatchIndexManager();
        }
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
                QueryDataSet dataSet = overflowQueryEngine.getDataInTsFile(columnPath, fileInfo.getFilePath());
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
                IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath, new IndexConfig());

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
            boolean overallResult = true;
            for (DataFileMultiSeriesInfo fileInfo : newFileList) {
                List<Future<Boolean>> results = new ArrayList<>(fileInfo.getColumnPaths().size());
                for (int i = 0; i < fileInfo.getColumnPaths().size(); i++) {
                    Path columnPath = fileInfo.getColumnPaths().get(i);
                    Pair<Long, Long> timeRange = fileInfo.getTimeRanges().get(i);

                    // 0. get configuration from store
                    IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath, new IndexConfig());
                    if (timeRange.right < indexConfig.getSinceTime()) continue;  // not in index range

                    // 1. build index for every data series.
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
        Set<String> newIndexFilePathPrefixes = new HashSet<>(newFileList.size());
        // get all index file path should be left
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
            IndexConfig indexConfig = indexConfigStore.getOrDefault(columnPath, new IndexConfig());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, queryRequest.getStartTime());

            // 2. fetch non-indexed ranges from overflow manager
            OverflowBufferWrite overflowBufferWrite = overflowQueryEngine.getDataInBufferWriteSeparateWithOverflow(columnPath);

            // 3. propagate query series and configurations
            KvMatchQueryRequest request = (KvMatchQueryRequest) queryRequest;
            List<Double> querySeries = getQuerySeries(request);
            if (querySeries.size() < 2 * indexConfig.getWindowLength() - 1) {
                throw new IllegalArgumentException("The length of query series can not shorter than 2*<window_length>-1 (" + querySeries.size() + " < " + (2 * indexConfig.getWindowLength() - 1) +")");
            }
            QueryConfig queryConfig = new QueryConfig(indexConfig, querySeries, request.getEpsilon(), request.getAlpha(), request.getBeta());

            // 4. search corresponding index files of data files in the query range
            List<Future<QueryResult>> futureResults = new ArrayList<>(fileInfoList.size());
            for (DataFileInfo fileInfo : fileInfoList) {
                if (fileInfo.getEndTime() <= overflowBufferWrite.getDeleteUntil()) continue;  // deleted
                if (fileInfo.getStartTime() > queryRequest.getEndTime() || fileInfo.getEndTime() < queryRequest.getStartTime())
                    continue;  // not in query range
                if (fileInfo.getEndTime() < indexConfig.getSinceTime())
                    continue;  // not indexed files are not allowed to query
                File indexFile = new File(IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
                if (indexFile.exists()) {
                    KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryConfig, columnPath, indexFile.getAbsolutePath());
                    Future<QueryResult> result = executor.submit(queryExecutor);
                    futureResults.add(result);
                } else {  // the index of this file has not been built, this will not happen in normal circumstance (likely to happen between close operation and index building of new file finished)
                    overflowBufferWrite.getInsertOrUpdateIntervals().add(fileInfo.getTimeInterval().get(0));
                    overflowBufferWrite.setInsertOrUpdateIntervals(IntervalUtils.sortAndMergePair(overflowBufferWrite.getInsertOrUpdateIntervals()));
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
            overallResult.setCandidateRanges(IntervalUtils.sortAndMergePair(overallResult.getCandidateRanges()));
            overallResult.setCandidateRanges(IntervalUtils.union(overallResult.getCandidateRanges(), overflowBufferWrite.getInsertOrUpdateIntervals()));
            logger.info("Candidates: {}", overallResult.getCandidateRanges());

            // 7. scan the data in candidate ranges and find out actual answers
            List<Pair<Long, Long>> scanIntervals = IntervalUtils.extendAndMerge(overallResult.getCandidateRanges(), querySeries.size());
            QueryDataSet dataSet = overflowQueryEngine.query(columnPath, scanIntervals);
            List<Pair<Pair<Long, Long>, Double>> answers = validateCandidates(scanIntervals, dataSet, querySeries, request);
            answers.sort(Comparator.comparingDouble(o -> o.right));
            logger.info("Answers: {}", answers);

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

    private List<Double> getQuerySeries(KvMatchQueryRequest request) throws ProcessorException, PathErrorException, IOException {
        List<Pair<Long, Long>> timeInterval = new ArrayList<>(Collections.singleton(new Pair<>(request.getQueryStartTime(), request.getQueryEndTime())));
        QueryDataSet dataSet = overflowQueryEngine.query(request.getQueryPath(), timeInterval);
        List<Pair<Long, Double>> keyPoints = new ArrayList<>();
        while (dataSet.next()) {
            RowRecord row = dataSet.getCurrentRecord();
            keyPoints.add(new Pair<>(row.getTime(), Double.parseDouble(row.getFields().get(0).getStringValue())));
        }
        return amendSeries(keyPoints);
    }

    private List<Pair<Pair<Long, Long>, Double>> validateCandidates(List<Pair<Long, Long>> scanIntervals, QueryDataSet dataSet, List<Double> querySeries, KvMatchQueryRequest request) {
        List<Pair<Pair<Long, Long>, Double>> result = new ArrayList<>();

        // do z-normalization on query data
        int lenQ = querySeries.size();
        List<Double> normalizedQuerySeries = new ArrayList<>(lenQ);
        double ex = 0, ex2 = 0;
        for (double value : querySeries) {
            ex += value;
            ex2 += value * value;
        }
        double meanQ = ex / lenQ, stdQ = Math.sqrt(ex2 / lenQ - meanQ * meanQ);
        for (double value : querySeries) {
            normalizedQuerySeries.add((value - meanQ) / stdQ);
        }
        // sort the query data
        List<Integer> order = new ArrayList<>(lenQ);
        List<Pair<Double, Integer>> tmpQuery = new ArrayList<>(lenQ);
        for (int i = 0; i < lenQ; i++) {
            tmpQuery.add(new Pair<>(normalizedQuerySeries.get(i), i));
        }
        tmpQuery.sort((o1, o2) -> o2.left.compareTo(o1.left));
        for (int i = 0; i < lenQ; i++) {
            normalizedQuerySeries.set(i, tmpQuery.get(i).left);
            order.add(tmpQuery.get(i).right);
        }

        // find answers in candidate ranges
        Pair<Long, Double> lastKeyPoint = null;
        for (Pair<Long, Long> scanInterval : scanIntervals) {
            List<Pair<Long, Double>> keyPoints = new ArrayList<>();
            if (!dataSet.hasNextRecord()) break;
            while (dataSet.next()) {
                RowRecord row = dataSet.getCurrentRecord();
                double value = Double.parseDouble(row.getFields().get(0).getStringValue());
                if (keyPoints.isEmpty() && row.getTime() > scanInterval.left) {
                    if (lastKeyPoint == null) {
                        keyPoints.add(new Pair<>(scanInterval.left, value));
                    } else {
                        keyPoints.add(lastKeyPoint);
                    }
                }
                keyPoints.add(new Pair<>(row.getTime(), value));
                if (row.getTime() >= scanInterval.right) break;
            }
            lastKeyPoint = keyPoints.get(keyPoints.size() - 1);
            List<Double> series = amendSeries(keyPoints, scanInterval);

            ex = 0;
            ex2 = 0;
            int idx = 0;
            double[] T = new double[2 * lenQ];
            for (int i = 0; i < series.size(); i++) {
                double value = series.get(i);
                ex += value;
                ex2 += value * value;
                T[i % lenQ] = value;
                T[(i % lenQ) + lenQ] = value;

                if (i >= lenQ - 1) {
                    int j = (i + 1) % lenQ;  // the current starting location of T

                    long left = scanInterval.left + i - lenQ + 1;
                    if (left == keyPoints.get(idx).left) {  // remove non-exist timestamp
                        idx++;
                        double mean = ex / lenQ;  // z
                        double std = Math.sqrt(ex2 / lenQ - mean * mean);

                        if ((request.getAlpha() == 1.0 && request.getBeta() == 0) ||
                                (Math.abs(mean - meanQ) <= request.getBeta() && std / stdQ <= request.getBeta() && std / stdQ >= 1.0 / request.getAlpha())) {
                            double dist = 0;
                            for (int k = 0; k < lenQ && dist <= request.getEpsilon() * request.getEpsilon(); k++) {
                                double x = (T[(order.get(k) + j)] - mean) / std;
                                dist += (x - normalizedQuerySeries.get(k)) * (x - normalizedQuerySeries.get(k));
                            }
                            if (dist <= request.getEpsilon() * request.getEpsilon()) {
                                result.add(new Pair<>(new Pair<>(left, scanInterval.left + i), Math.sqrt(dist)));
                            }
                        }
                    }

                    ex -= T[j];
                    ex2 -= T[j] * T[j];
                }
            }
        }

        return result;
    }

    private List<Double> amendSeries(List<Pair<Long, Double>> keyPoints, Pair<Long, Long> interval) {
        List<Double> ret = new ArrayList<>();
        if (keyPoints.get(0).left >= interval.left) ret.add(keyPoints.get(0).right);
        for (int i = 1; i < keyPoints.size(); i++) {
            double k = 1.0 * (keyPoints.get(i).right - keyPoints.get(i - 1).right) / (keyPoints.get(i).left - keyPoints.get(i - 1).left);
            for (long j = keyPoints.get(i - 1).left + 1; j <= keyPoints.get(i).left; j++) {
                if (j >= interval.left && j <= interval.right) {
                    ret.add(keyPoints.get(i - 1).right + (j - keyPoints.get(i - 1).left) * k);
                }
            }
        }
        return ret;
    }

    private List<Double> amendSeries(List<Pair<Long, Double>> keyPoints) {
        return amendSeries(keyPoints, new Pair<>(keyPoints.get(0).left, keyPoints.get(keyPoints.size() - 1).left));
    }
}
