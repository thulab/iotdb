package cn.edu.tsinghua.tsfile

import java.io.File

import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class MyTest extends FunSuite with BeforeAndAfterAll {
  private val resourcesFolder = "src/test/resources"
  private val tsfileFolder = "src/test/resources/tsfile"
  private val tsfile1 = "src/test/resources/tsfile/test1.tsfile"
  private val tsfile2 = "src/test/resources/tsfile/test2.tsfile"
  private val outputPath = "src/test/resources/output"
  private val outputPathFile = outputPath + "/part-m-00000"
  private val outputPath2 = "src/test/resources/output2"
  private val outputPathFile2 = outputPath2 + "/part-m-00000"
  private var spark: SparkSession = _

  def deleteDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.list().foreach(f => {
        deleteDir(new File(dir, f))
      })
    }
    dir.delete()

  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    //    val resources = new File(resourcesFolder)
    //    if (!resources.exists())
    //      resources.mkdirs()
    //    val tsfile_folder = new File(tsfileFolder)
    //    if (!tsfile_folder.exists())
    //      tsfile_folder.mkdirs()
    //    val output = new File(outputPath)
    //    if (output.exists())
    //      deleteDir(output)
    //    val output2 = new File(outputPath2)
    //    if (output2.exists())
    //      deleteDir(output2)
    //    new CreateTSFile().createTSFile1(tsfile1)
    //    new CreateTSFile().createTSFile2(tsfile2)
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    val out = new File(outputPath)
    deleteDir(out)
    val out2 = new File(outputPath2)
    deleteDir(out2)
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }


  test("writer") {
    val df = spark.read.tsfile("test1.tsfile")
    df.show()
    df.write.tsfile(outputPath)
    val newDf = spark.read.tsfile(outputPathFile)
    newDf.show()
    Assert.assertEquals(newDf.collectAsList(), df.collectAsList())
  }


}
