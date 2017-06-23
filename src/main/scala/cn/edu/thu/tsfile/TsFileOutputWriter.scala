package cn.edu.thu.tsfile

import cn.edu.thu.tsfile.io.HDFSOutputStream
import cn.edu.thu.tsfile.timeseries.FileFormat.TsFile
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.json.JSONObject


private[tsfile] class TsFileOutputWriter(path: String, context: TaskAttemptContext) extends OutputWriter{

  private val tsfile = {
    val conf = context.getConfiguration
    val hdfsOutput = new HDFSOutputStream(path, conf, true)
    new TsFile(hdfsOutput, new JSONObject(""))
  }

  override def write(row: Row): Unit = {
    tsfile.writeLine("")
  }

  override def close(): Unit = {
    tsfile.close()
  }
}
