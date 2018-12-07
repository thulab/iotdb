package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.utils.MetadataGenerator;
import cn.edu.tsinghua.tsfile.utils.CompareUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkGroupMetaDataTest {

  public static final String DELTA_OBJECT_UID = "delta-3312";
  final String PATH = "target/outputChunkGroup.tsfile";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    ChunkGroupMetaData metaData = MetadataGenerator.createSimpleChunkGroupMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    metaData.serializeTo(fos);
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    CompareUtils.isChunkGroupMetaDataEqual(metaData, ChunkGroupMetaData.deserializeFrom(fis));
  }
}
