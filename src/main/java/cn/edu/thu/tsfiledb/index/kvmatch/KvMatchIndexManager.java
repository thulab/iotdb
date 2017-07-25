package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchIndexBuilder;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchQueryExecutor;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryResult;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IntervalUtils;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.*;
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

    private KvMatchIndexManager() {
        executor = Executors.newFixedThreadPool(2);  // TODO: resource management
        overflowQueryEngine = new OverflowQueryEngine();
    }

    public static KvMatchIndexManager getInstance() {
        if (manager == null) {
            manager = new KvMatchIndexManager();
        }
        return manager;
    }

    public static void main(String args[]) {  // for temporarily test only
        KvMatchIndexManager indexManager = KvMatchIndexManager.getInstance();
        try {
            Path columnPath = new Path("root.laptop.d1.s1");

            indexManager.build(columnPath);

            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, 0);

            indexManager.rebuild(columnPath, fileInfoList);

            indexManager.switchIndexes(columnPath, fileInfoList);

            OverflowQueryEngine overflowQueryEngine = new OverflowQueryEngine();
            List<Pair<Long, Long>> timeIntervals = new ArrayList<>();
            timeIntervals.add(new Pair<>(1500885911634L, 1500885911634L + 512 - 1));
            QueryDataSet queryDataSet = overflowQueryEngine.query(columnPath, timeIntervals);
            List<Pair<Long, Double>> querySeries = new ArrayList<>();
            while (queryDataSet.next()) {
                querySeries.add(new Pair<>(queryDataSet.getCurrentRecord().getTime(), Double.parseDouble(queryDataSet.getCurrentRecord().getFields().get(0).getStringValue())));
            }
            KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, 1.0).alpha(1.0).beta(0.0).build();
            indexManager.query(queryRequest);

            indexManager.delete(columnPath);
        } catch (IndexManagerException | FileNodeManagerException | ProcessorException | PathErrorException | IOException e) {
            logger.error(e.getMessage(), e.getCause());
        } finally {
            indexManager.executor.shutdown();
        }
    }

    @Override
    public boolean build(Path columnPath) throws IndexManagerException {
        return build(columnPath, 0, new HashMap<>());
    }

    @Override
    public boolean build(Path columnPath, long sinceTime, Map<String, Integer> parameters) throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, sinceTime);

            // 2. build index for every data file.
            IndexConfig indexConfig = new IndexConfig();
            indexConfig.setWindowLength(parameters.getOrDefault(IndexConfig.PARAM_WINDOW_LENGTH, IndexConfig.DEFAULT_WINDOW_LENGTH));
            List<Future<Boolean>> results = new ArrayList<>(fileInfoList.size());
            for (DataFileInfo fileInfo : fileInfoList) {
                QueryDataSet dataSet = overflowQueryEngine.query(columnPath, fileInfo.getTimeInterval());
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
            return overallResult;
        } catch (FileNodeManagerException | IOException | ProcessorException | PathErrorException | InterruptedException | ExecutionException e) {
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
            return true;
        } catch (FileNodeManagerException e) {
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
    public boolean rebuild(Path columnPath, List<DataFileInfo> modifiedFileList) throws IndexManagerException {
        try {
            // 1. build index for every data file.
            List<Future<Boolean>> results = new ArrayList<>(modifiedFileList.size());
            for (DataFileInfo fileInfo : modifiedFileList) {
                QueryDataSet dataSet = overflowQueryEngine.query(columnPath, fileInfo.getTimeInterval());
                Future<Boolean> result = executor.submit(new KvMatchIndexBuilder(new IndexConfig(), columnPath, dataSet, IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()) + ".new"));
                results.add(result);
            }

            // 2. collect building results.
            boolean overallResult = true;
            for (Future<Boolean> result : results) {
                if (!result.get()) {
                    overallResult = false;
                }
            }
            return overallResult;
        } catch (IOException | ProcessorException | PathErrorException | InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        }
    }

    @Override
    public boolean switchIndexes(Path columnPath, List<DataFileInfo> newFileList) throws IndexManagerException {
        if (newFileList.isEmpty()) return true;  // no data file, no index file
        // rename the new index files to regular names
        Set<String> newIndexFilePathPrefixes = new HashSet<>(newFileList.size());
        for (DataFileInfo newFile : newFileList) {
            newIndexFilePathPrefixes.add(IndexFileUtils.getIndexFilePathPrefix(newFile.getFilePath()));
            String filename = IndexFileUtils.getIndexFilePath(columnPath, newFile.getFilePath());
            File indexFile = new File(filename + ".new");
            if (!indexFile.renameTo(new File(filename))) {
                logger.error("Can not rename new index file '{}'", filename);
                return false;
            }
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
    public QueryResponse query(QueryRequest queryRequest) throws IndexManagerException {
        Path columnPath = queryRequest.getColumnPath();
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, queryRequest.getStartTime());

            // 2. fetch non-indexed ranges from overflow manager
            OverflowBufferWrite overflowBufferWrite = overflowQueryEngine.getDataInBufferWriteSeparateWithOverflow(columnPath);

            // 3. propagate query series
            List<Double> querySeries = amendSeries(queryRequest.getQuerySeries());

            // 4. search corresponding index files of data files in the query range
            List<Future<QueryResult>> futureResults = new ArrayList<>(fileInfoList.size());
            for (DataFileInfo fileInfo : fileInfoList) {
                if (fileInfo.getEndTime() <= overflowBufferWrite.getDeleteUntil()) continue;  // deleted
                logger.info("Querying index for '{}': [{}, {}] ({})", columnPath, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());
                QueryConfig queryConfig = new QueryConfig(querySeries, ((KvMatchQueryRequest) queryRequest).getEpsilon());
                KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryConfig, columnPath, IndexFileUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
                Future<QueryResult> result = executor.submit(queryExecutor);
                futureResults.add(result);
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
            QueryDataSet dataSet = overflowQueryEngine.query(columnPath, IntervalUtils.extendAndMerge(overallResult.getCandidateRanges(), querySeries.size()));
            List<Pair<Pair<Long, Long>, Double>> answers = validateCandidates(dataSet, overflowBufferWrite.getBufferWriteData());
            return new KvMatchQueryResponse(answers);
        } catch (FileNodeManagerException | InterruptedException | ExecutionException | ProcessorException | IOException | PathErrorException e) {
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

    private List<Pair<Pair<Long, Long>, Double>> validateCandidates(QueryDataSet dataSet, QueryDataSet bufferDataSet) {
        return null;
    }

    private List<Double> amendSeries(List<Pair<Long, Double>> seriesKeyPoints) {
        List<Double> ret = new ArrayList<>();
        ret.add(seriesKeyPoints.get(0).right);
        for (int i = 1; i < seriesKeyPoints.size(); i++) {
            // amend points on the line
            double k = 1.0 * (seriesKeyPoints.get(i).right - seriesKeyPoints.get(i - 1).right) / (seriesKeyPoints.get(i).left - seriesKeyPoints.get(i - 1).left);
            for (long j = seriesKeyPoints.get(i - 1).left + 1; j < seriesKeyPoints.get(i).left; j++) {
                ret.add(seriesKeyPoints.get(i - 1).right + (j - seriesKeyPoints.get(i - 1).left) * k);
            }
            ret.add(seriesKeyPoints.get(i).right);  // add current point
        }
        return ret;
    }
}
