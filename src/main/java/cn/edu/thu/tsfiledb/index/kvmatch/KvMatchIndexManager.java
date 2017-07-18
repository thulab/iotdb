package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.IndexManager;
import cn.edu.thu.tsfiledb.index.QueryRequest;
import cn.edu.thu.tsfiledb.index.QueryResponse;
import cn.edu.thu.tsfiledb.metadata.MManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The class manage the indexes of KV-match.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexManager implements IndexManager {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexManager.class);

    private static KvMatchIndexManager manager = null;

    private String fileNodeDir;
    private String indexFileDir;

    private KvMatchIndexManager() {
        fileNodeDir = TsfileDBDescriptor.getInstance().getConfig().fileNodeDir;
        indexFileDir = TsfileDBDescriptor.getInstance().getConfig().indexFileDir;
    }

    public static KvMatchIndexManager getInstance() {
        if (manager == null) {
            manager = new KvMatchIndexManager();
        }
        return manager;
    }

    public static void main(String args[]) {  // for temporarily test only
        // TODO: produce synthetic data

        KvMatchIndexManager indexManager = KvMatchIndexManager.getInstance();
        try {
            String columnPath = "root.excavator.shanghai.d1.s1";

            indexManager.build(columnPath);

            List<Pair<Long, Double>> querySeries = new ArrayList<>();
            KvMatchQueryRequest queryRequest = KvMatchQueryRequest.builder(columnPath, querySeries, 1.0).alpha(1.0).beta(0.0).build();
            indexManager.query(queryRequest);

            List<File> fileList = new ArrayList<>();
            indexManager.rebuild(fileList);
        } catch (PathErrorException e) {
            logger.error(e.getMessage(), e.getCause());
        }
    }

    /**
     * Build index for the given column path.
     *
     * @param columnPath building index for this column path
     * @return whether the index building process is successful
     * @throws PathErrorException if the given column path is not valid
     */
    @Override
    public boolean build(String columnPath) throws PathErrorException {
        return build(columnPath, Long.MIN_VALUE);
    }

    /**
     * Build index for the given column path after specific time.
     *
     * @param columnPath building index for this column path
     * @param sinceTime  only build index for data after this time
     * @return whether the index building process is successful
     * @throws PathErrorException if the given column path is not valid
     */
    @Override
    public boolean build(String columnPath, long sinceTime) throws PathErrorException {
        // 1. get information of file node according to column path
        String fileNodeName = MManager.getInstance().getFileNameByPath(columnPath);

        // 2. get information of all files in the file node directory
        Path path = FileSystems.getDefault().getPath(fileNodeDir, fileNodeName);

        // 3. build index for every data file. TODO: using multi-thread to speed up
        try {
            for (Path file : Files.newDirectoryStream(path)) {
                logger.info("Building index for data file `{}` ...", file.toString());
                KvMatchIndexBuilder indexBuilder = new KvMatchIndexBuilder(file);
                indexBuilder.build();
            }
        } catch (NoSuchFileException e) {
            logger.error("There is no data file of `{}`. Data should be flushed before building index.", columnPath);
            return false;
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
            return false;
        }
        return true;
    }

    @Override
    public QueryResponse query(QueryRequest queryRequest) {
        KvMatchQueryExecutor queryExecutor = new KvMatchQueryExecutor(queryRequest);
        return queryExecutor.execute();
    }

    @Override
    public boolean rebuild(List<File> fileList) {
        return false;
    }
}
