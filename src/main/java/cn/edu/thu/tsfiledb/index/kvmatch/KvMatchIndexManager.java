package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.KvMatchIndexBuilder;
import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.DataFileInfo;
import cn.edu.thu.tsfiledb.index.IndexManager;
import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.QueryResponse;
import cn.edu.thu.tsfiledb.index.utils.IndexUtils;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The class manage the indexes of KV-match.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexManager.class);

    private static KvMatchIndexManager manager = null;

    private OverflowQueryEngine overflowQueryEngine;

    private KvMatchIndexManager() {
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

            indexManager.rebuild(columnPath, FileNodeManager.getInstance().indexBuildQuery(columnPath, 0));

            indexManager.switchIndexes(columnPath, FileNodeManager.getInstance().indexBuildQuery(columnPath, 0));

            List<Pair<Long, Double>> querySeries = new ArrayList<>();
            int value = ThreadLocalRandom.current().nextInt(-5, 5);
            for (int i = 0; i < 128; i++) {
                querySeries.add(new Pair<>((long) i, (double) value));
                value += ThreadLocalRandom.current().nextInt(-1, 1);
            }
            KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, 1.0).alpha(1.0).beta(0.0).build();
            indexManager.query(queryRequest);

            indexManager.delete(columnPath);
        } catch (IndexManagerException | FileNodeManagerException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    @Override
    public boolean build(Path columnPath) throws IndexManagerException {
        return build(columnPath, 0);
    }

    @Override
    public boolean build(Path columnPath, long sinceTime) throws IndexManagerException {
        int token = -1;
        try {
            token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());

            // 1. get information of all files containing this column path.
            List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, sinceTime);

            // 2. build index for every data file. TODO: using multi-thread to speed up
            KvMatchIndexBuilder indexBuilder = new KvMatchIndexBuilder(columnPath);
            for (DataFileInfo fileInfo : fileInfoList) {
                logger.info("Building index for '{}': [{}, {}] ({})", columnPath, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());
                QueryDataSet dataSet = overflowQueryEngine.query(columnPath, fileInfo.getTimeInterval());
                indexBuilder.build(dataSet, IndexUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
            }
            return true;
        } catch (FileNodeManagerException | IOException | ProcessorException | PathErrorException e) {
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

            // 2. delete all index files. TODO: using multi-thread to speed up
            for (DataFileInfo fileInfo : fileInfoList) {
                logger.info("Deleting index for '{}': [{}, {}] ({})", columnPath, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());

                File indexFile = new File(IndexUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()));
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
            KvMatchIndexBuilder indexBuilder = new KvMatchIndexBuilder(columnPath);
            for (DataFileInfo fileInfo : modifiedFileList) {
                logger.info("Building index for '{}': [{}, {}] ({})", columnPath, fileInfo.getStartTime(), fileInfo.getEndTime(), fileInfo.getFilePath());
                QueryDataSet dataSet = overflowQueryEngine.query(columnPath, fileInfo.getTimeInterval());
                indexBuilder.build(dataSet, IndexUtils.getIndexFilePath(columnPath, fileInfo.getFilePath()) + ".new");
            }
            return true;
        } catch (IOException | ProcessorException | PathErrorException e) {
            logger.error(e.getMessage(), e.getCause());
            throw new IndexManagerException(e);
        }
    }

    @Override
    public boolean switchIndexes(Path columnPath, List<DataFileInfo> newFileList) throws IndexManagerException {
        // rename the new index files to regular names
        Set<String> newIndexFilePathPrefixes = new HashSet<>(newFileList.size());
        for (DataFileInfo newFile : newFileList) {
            newIndexFilePathPrefixes.add(IndexUtils.getIndexFilePathPrefix(newFile.getFilePath()));
            String filename = IndexUtils.getIndexFilePath(columnPath, newFile.getFilePath());
            File indexFile = new File(filename + ".new");
            if (!indexFile.renameTo(new File(filename))) {
                logger.error("Can not rename new index file '{}'", filename);
                return false;
            }
        }
        // get all exist index files, and delete files not in new file list
        File indexFileDir = new File(IndexUtils.getIndexFilePathPrefix(newFileList.get(0).getFilePath())).getParentFile();
        File[] indexFiles = indexFileDir.listFiles();
        if (indexFiles != null) {
            for (File file : indexFiles) {
                if (!newIndexFilePathPrefixes.contains(IndexUtils.getIndexFilePathPrefix(file))) {
                    if (!file.delete()) {
                        logger.warn("Can not delete obsolete index file '{}'", file);
                    }
                }
            }
        }
        return true;
    }

    @Override
    public QueryResponse query(QueryRequest queryRequest) {
        // 1. fetch non-indexed ranges from overflow manager

        // 2. fetch TsFile data file list from file node manager

        // 3. search corresponding index files of data files in the query range
//        KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryRequest);
//        return queryExecutor.execute();

        // 4. merge the candidate ranges and non-indexed ranges to produce candidate ranges

        // 5. scan the data in candidate ranges and find out actual answers
        return new KvMatchQueryResponse();
    }
}
