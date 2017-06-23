package cn.edu.thu.tsfile

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType

/**
  * @author qiaojialin
  */
private[tsfile] class TsFileWriterFactory() extends OutputWriterFactory{

  override def newInstance(
                            path: String,
                            bucketId: Option[Int],
                            dataSchema: StructType,
                            context: TaskAttemptContext): OutputWriter = {
    println("---newInstance---")

    println(dataSchema)

    new TsFileOutputWriter(path, context)
  }
}
