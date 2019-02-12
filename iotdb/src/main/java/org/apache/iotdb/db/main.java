package org.apache.iotdb.db;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.iotdb.db.engine.cache.TsFileMetadataUtils;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class main {

  public static void main(String[] args) throws IOException {
    String path = "D:/github/debt/iotdb/test2.tsfile";
    TsFileMetaData tsFileMetaData = TsFileMetadataUtils
        .getTsFileMetaData(path);
    System.out.println("");

    TsFileSequenceReader reader = new TsFileSequenceReader(path);
    ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
    ArrayList<Path> paths = new ArrayList<>();
    paths.add(new Path("device_1.sensor_1"));
    paths.add(new Path("device_2.sensor_1"));
    paths.add(new Path("device_2.sensor_2"));
    QueryExpression queryExpression = QueryExpression.create(paths, null);

    QueryDataSet queryDataSet = readTsFile.query(queryExpression);
    for (int j = 0; j < paths.size(); j++) {
//      assertEquals(paths.get(j), queryDataSet.getPaths().get(j));
      System.out.println(queryDataSet.getPaths().get(j));
    }

    int i = 1;
    while (queryDataSet.hasNext()) {
      RowRecord r = queryDataSet.next();
//      assertEquals(i, r.getTimestamp());
//      assertEquals(i, r.getFields().get(0).getIntV());
      i++;
    }
    reader.close();

//    SparkSession spark = SparkSession
//        .builder()
//        .config("spark.master", "local")
//        .appName("TSFile test")
//        .getOrCreate();
//    SparkContext sc = spark.sparkContext();
//    Configuration configuration = sc.hadoopConfiguration();
//    FileSystem fs = FileSystem.get(configuration);
//    FSDataInputStream fsDataInputStream = fs.open(new Path("D:/github/debt/iotdb/test.tsfile"));
//    TsFileMetaData tsFileMetaData = TsFileMetaData.deserializeFrom(fsDataInputStream);
  }

}

