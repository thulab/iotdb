package cn.edu.tsinghua.iotdb.query.control;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class FileReaderManagerTest {

    private static final int MAX_FILE_SIZE = 10;

    @Test
    public void test() throws IOException, InterruptedException {

        String filePath = "target/test.file";

        FileReaderManager manager = FileReaderManager.getInstance();

        for (int i = 0; i < MAX_FILE_SIZE; i++) {
            File file = new File(filePath + i);
            file.createNewFile();
        }

        for (int i = 0; i < MAX_FILE_SIZE; i++) {
            manager.get(filePath + i, true);
            Assert.assertTrue(manager.contains(filePath + i));
        }

        TimeUnit.SECONDS.sleep(6);

//        for (int i = 0; i < MAX_FILE_SIZE; i++) {
//            Assert.assertFalse(manager.contains(filePath + i));
//        }

        for (int i = 0; i < MAX_FILE_SIZE; i++) {
            File file = new File(filePath + i);
            file.delete();
        }
    }
}
