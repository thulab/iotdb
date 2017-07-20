package cn.edu.thu.tsfiledb.index.utils;

import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;

import java.io.File;

/**
 * The utility class to convert data file path to index file path.
 *
 * @author Jiaye Wu
 */
public class IndexUtils {

    private static final String DATA_FILE_PATH, INDEX_FILE_PATH;

    static {
        TsfileDBConfig config = new TsfileDBConfig();
        DATA_FILE_PATH = File.separator + config.bufferWriteDir + File.separator;
        INDEX_FILE_PATH = File.separator + config.indexFileDir + File.separator;
    }

    public static String getIndexFilePath(String dataFilePath) {
        return dataFilePath.replace(DATA_FILE_PATH, INDEX_FILE_PATH);
    }
}
