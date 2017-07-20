package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.DataFileInfo;
import cn.edu.thu.tsfiledb.index.IndexManager;
import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The class manage the indexes of KV-match.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexManager.class);

    private static KvMatchIndexManager manager = null;

    private String dataFileDir, indexFileDir;

    private KvMatchIndexManager() {
        dataFileDir = TsfileDBDescriptor.getInstance().getConfig().bufferWriteDir;
        indexFileDir = TsfileDBDescriptor.getInstance().getConfig().indexFileDir;
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

            List<Pair<Long, Double>> querySeries = new ArrayList<>();
            int value = ThreadLocalRandom.current().nextInt(-5, 5);
            for (int i = 0; i < 128; i++) {
                querySeries.add(new Pair<>((long) i, (double) value));
                value += ThreadLocalRandom.current().nextInt(-1, 1);
            }
            KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, 1.0).alpha(1.0).beta(0.0).build();
            indexManager.query(queryRequest);
        } catch (PathErrorException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    @Override
    public boolean build(Path columnPath) throws PathErrorException {
        return build(columnPath, 0);
    }

    @Override
    public boolean build(Path columnPath, long sinceTime) throws PathErrorException {
        // 1. get information of all files containing this column path. TODO: pending for API
//        List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().XXX(columnPath, sinceTime);
    	int token = FileNodeManager.getInstance().beginQuery(columnPath.getDeltaObjectToString());
    	 List<DataFileInfo> fileInfoList  = FileNodeManager.getInstance().indexBuildQuery(columnPath, sinceTime);
    	
    	

        // 2. build index for every data file. TODO: using multi-thread to speed up
        try {
            for (DataFileInfo fileInfo : fileInfoList) {
                logger.info("Building index for data file `{}` ...", fileInfo.getFile().toString());
                KvMatchIndexBuilder indexBuilder = new KvMatchIndexBuilder(columnPath, fileInfo, true);
                indexBuilder.build();
            }
        } catch (IOException | ProcessorException e) {
            logger.error(e.getMessage(), e.getCause());
            return false;
        }
        FileNodeManager.getInstance().endQuery(columnPath.getDeltaObjectToString(), token);
        return true;
    }

    @Override
    public boolean delete(Path columnPath) throws PathErrorException {
        return false;
    }

    @Override
    public boolean rebuild(Path columnPath, List<DataFileInfo> modifiedFileList) throws PathErrorException {
        try {
            for (DataFileInfo fileInfo : modifiedFileList) {
                logger.info("Building index for data file `{}` ...", fileInfo.getFile().toString());
                KvMatchIndexBuilder indexBuilder = new KvMatchIndexBuilder(columnPath, fileInfo, false);
                indexBuilder.build();
            }
        } catch (IOException | ProcessorException e) {
            logger.error(e.getMessage(), e.getCause());
            return false;
        }
        return true;
    }

    @Override
    public boolean switchIndexes(Path columnPath, List<DataFileInfo> newFileList) throws PathErrorException {
        return false;
    }

    @Override
    public QueryResponse query(QueryRequest queryRequest) {
        KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryRequest);
        return queryExecutor.execute();
    }
}
