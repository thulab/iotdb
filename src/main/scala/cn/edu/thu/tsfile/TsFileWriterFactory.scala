package cn.edu.thu.tsfile

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
  * @author qiaojialin
  */
private[tsfile] class TsFileWriterFactory(options: Map[String, String]) extends OutputWriterFactory{

  override def newInstance(
                            path: String,
                            bucketId: Option[Int],
                            dataSchema: StructType,
                            context: TaskAttemptContext): OutputWriter = {
    new TsFileOutputWriter(path, dataSchema, options, context)
  }
}
