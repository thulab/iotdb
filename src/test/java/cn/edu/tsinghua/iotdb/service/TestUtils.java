package cn.edu.tsinghua.iotdb.service;


import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class TestUtils {

    static boolean testFlag = true;

    static String count(String path) {
        return String.format("count(%s)", path);
    }

    static String max_time(String path) {
        return String.format("max_time(%s)", path);
    }

    static String min_time(String path) {
        return String.format("min_time(%s)", path);
    }

    static String max_value(String path) {
        return String.format("max_value(%s)", path);
    }

    static String min_value(String path) {
        return String.format("min_value(%s)", path);
    }

    static void clearDir(TsfileDBConfig config, String FOLDER_HEADER) throws IOException {
        FileUtils.deleteDirectory(new File(config.overflowDataDir));
        FileUtils.deleteDirectory(new File(config.fileNodeDir));
        FileUtils.deleteDirectory(new File(config.bufferWriteDir));
        FileUtils.deleteDirectory(new File(config.metadataDir));
        FileUtils.deleteDirectory(new File(config.derbyHome));
        FileUtils.deleteDirectory(new File(config.walFolder));
        FileUtils.deleteDirectory(new File(config.indexFileDir));
        FileUtils.deleteDirectory(new File(FOLDER_HEADER + "/data"));
    }
}
