package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TsFileMetaDataTest {
    final String PATH = "target/output1.tsfile";
    public static final int VERSION = 123;
    public static final String CREATED_BY = "tsf";

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    }

    @Test
    public void testWriteFileMetaData() throws IOException {
    TsFileMetaData tsfMetaData = TestHelper.createSimpleFileMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    tsfMetaData.serializeTo(fos);
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isFileMetaDataEqual(tsfMetaData, TsFileMetaData.deserializeFrom(fis));
    }
}
