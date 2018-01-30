package cn.edu.tsinghua.iotdb.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class OpenFileNumUtilTest {
    private OpenFileNumUtil openFileNumUtil = OpenFileNumUtil.getInstance();
    private ArrayList<File> fileList = new ArrayList<>();
    private ArrayList<FileWriter> fileWriterList = new ArrayList<>();
    private int totalOpenFileNumBefore;
    private int totalOpenFileNumAfter;
    private int totalOpenFileNumChange;
    private int testFileNum = 66;
    private String currDir;

    @Before
    public void setUp() throws Exception {
        openFileNumUtil.setPid(getProcessID());
        currDir = System.getProperty("user.dir");
    }

    @After
    public void tearDown() throws Exception {
        //close FileWriter
        for (FileWriter fw : fileWriterList) {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //delete test files
        for (File file : fileList) {
            if (file.exists()) {
                file.delete();
            }
        }

        fileWriterList.clear();
        fileList.clear();
    }


    @Test
    public void testTotalOpenFileNumWhenCreateFile() {
        //get total open file number statistics of original state
        totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        for (int i = 0; i < testFileNum; i++) {
            fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
        }
        //create testFileNum File，then get total open file number statistics
        totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
        //create test file shall not affect total open file number statistics
        assertEquals(0, totalOpenFileNumChange);
    }

    @Test
    public void testTotalOpenFileNumWhenCreateFileWriter() {
        for (int i = 0; i < testFileNum; i++) {
            fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
        }
        totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        for (File file : fileList) {
            if (file.exists()) {
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
        //create FileWriter shall cause total open file number increase by testFileNum
        assertEquals(testFileNum, totalOpenFileNumChange);
    }

    @Test
    public void testTotalOpenFileNumWhenFileWriterWriting() {
        for (int i = 0; i < testFileNum; i++) {
            fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
        }
        for (File file : fileList) {
            if (file.exists()) {
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        for (FileWriter fw : fileWriterList) {
            try {
                fw.write("this is a test file for open file number counting.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
        //writing test file shall not affect total open file number statistics
        assertEquals(0, totalOpenFileNumChange);
    }

    @Test
    public void testTotalOpenFileNumWhenFileWriterClose() {
        for (int i = 0; i < testFileNum; i++) {
            fileList.add(new File(currDir + "/testFileForOpenFileNumUtil" + i));
        }
        for (File file : fileList) {
            if (file.exists()) {
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    fileWriterList.add(new FileWriter(file));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        for (FileWriter fw : fileWriterList) {
            try {
                fw.write("this is a test file for open file number counting.");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        totalOpenFileNumBefore = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        for (FileWriter fw : fileWriterList) {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        totalOpenFileNumAfter = openFileNumUtil.get(OpenFileNumUtil.OpenFileNumStatistics.TOTAL_OPEN_FILE_NUM);
        totalOpenFileNumChange = totalOpenFileNumAfter - totalOpenFileNumBefore;
        //close FileWriter shall cause total open file number decrease by testFileNum
        assertEquals(-testFileNum, totalOpenFileNumChange);
    }


    private static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.parseInt(runtimeMXBean.getName().split("@")[0]);
    }

}
