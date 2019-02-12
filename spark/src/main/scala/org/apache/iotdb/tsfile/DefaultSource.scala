/**
  * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
  *
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
/**
  * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.iotdb.tsfile

import java.io.{ObjectInputStream, ObjectOutputStream, _}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.Job
import org.apache.iotdb.tsfile.qp.SQLConstant
import org.apache.iotdb.tsfile.read.common.{Field, RowRecord}
import org.apache.iotdb.tsfile.read.expression.QueryExpression
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet
import org.apache.iotdb.tsfile.read.{ReadOnlyTsFile, TsFileSequenceReader}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[tsfile] class DefaultSource extends FileFormat with DataSourceRegister {

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
                            spark: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    val conf = spark.sparkContext.hadoopConfiguration

    //check if the path is given
    options.getOrElse(DefaultSource.path, throw new TSFileDataSourceException(s"${DefaultSource.path} must be specified for org.apache.iotdb.tsfile DataSource"))

    //get union series in TsFile
    val tsfileSchema = Converter.getUnionSeries(files, conf)

    DefaultSource.columnNames.clear()

    //unfold delta_object
    if (options.contains(SQLConstant.DELTA_OBJECT_NAME)) {
      val columns = options(SQLConstant.DELTA_OBJECT_NAME).split(SQLConstant.REGEX_PATH_SEPARATOR)
      columns.foreach(f => {
        DefaultSource.columnNames += f
      })
    } else {
      //using delta_object
      DefaultSource.columnNames += SQLConstant.RESERVED_DELTA_OBJECT
    }

    Converter.toSqlSchema(tsfileSchema, DefaultSource.columnNames)
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: org.apache.hadoop.fs.Path): Boolean = {
    true
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    //    val broadcastedConf =
    //      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val log = LoggerFactory.getLogger(classOf[DefaultSource])
      log.info(file.toString())

      //      val conf = broadcastedConf.value.value

      //      val fs = FileSystem.get(conf)
      //      val fsDataInputStream = fs.open(new Path(new URI(file.filePath)))

      //      val in = new HDFSInputStream(new Path(new URI(file.filePath)), conf)

      val reader: TsFileSequenceReader = new TsFileSequenceReader(file.filePath) //TODO 这里没用hdfsinputstream因为没接口
      val readTsFile: ReadOnlyTsFile = new ReadOnlyTsFile(reader)

      Option(TaskContext.get()).foreach { taskContext => {
        taskContext.addTaskCompletionListener { _ => reader.close() } //TODO
        log.info("task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId())
      }
      }

      //      val parameters = new util.HashMap[java.lang.String, java.lang.Long]()
      //      parameters.put(QueryConstant.PARTITION_START_OFFSET, file.start.asInstanceOf[java.lang.Long])
      //      parameters.put(QueryConstant.PARTITION_END_OFFSET, (file.start + file.length).asInstanceOf[java.lang.Long])
      //
      //      //convert tsfile query to QueryConfigs
      //      val queryConfigs = Converter.toQueryConfigs(in, requiredSchema, filters, DefaultSource.columnNames,
      //        file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])
      //
      //      //use QueryConfigs to query data in tsfile
      //      val dataSets = Executor.query(in, queryConfigs.toList, parameters)

      //TODO 这里先不管查询 全部查询
      //TODO 并且这里queryDataSet不是dataSets那样的List<QueryDataSet>
      val paths: util.ArrayList[org.apache.iotdb.tsfile.read.common.Path] = new util.ArrayList[org.apache.iotdb.tsfile.read.common.Path]
      //      paths.add(new Path("device_1.sensor_1"))
      val queryExpression: QueryExpression = QueryExpression.create(paths, null)
      val dataSets = new util.ArrayList[QueryDataSet]()
      dataSets.add(readTsFile.query(queryExpression))


      case class Record(record: RowRecord, index: Int)

      implicit object RowRecordOrdering extends Ordering[Record] {
        override def compare(r1: Record, r2: Record): Int = {
          //          if (r1.record.getTimestamp == r2.record.getTimestamp) {
          //            r1.record.getFields.get(0).deltaObjectId.compareTo(r2.record.getFields.get(0).deltaObjectId)
          //          } else if (r1.record.getTimestamp < r2.record.getTimestamp) {
          //TODO 这里暂不考虑多个queryConfig带来的List<QueryDataSet>
          if (r1.record.getTimestamp < r2.record.getTimestamp) {
            1
          } else {
            -1
          }
        }
      }

      val priorityQueue = new mutable.PriorityQueue[Record]()

      //init priorityQueue with first record of each dataSet
      var queryDataSet: QueryDataSet = null
      for (i <- 0 until dataSets.size()) {
        queryDataSet = dataSets.get(i)
        if (queryDataSet.hasNext) {
          val rowRecord = queryDataSet.next()
          priorityQueue.enqueue(Record(rowRecord, i))
        }
      }

      var curRecord: Record = null

      new Iterator[InternalRow] {
        private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

        private val safeDataRow = new GenericRow(rowBuffer)

        // Used to convert `Row`s containing data columns into `InternalRow`s.
        private val encoderForDataColumns = RowEncoder(requiredSchema)

        private var deltaObjectId = "null"

        private var measurementIds = new util.ArrayList[String]() //TODO

        override def hasNext: Boolean = {
          var hasNext = false

          while (priorityQueue.nonEmpty && !hasNext) {
            //get a record from priorityQueue
            val tmpRecord = priorityQueue.dequeue()
            //insert a record to priorityQueue
            queryDataSet = dataSets.get(tmpRecord.index)
            if (queryDataSet.hasNext) {
              priorityQueue.enqueue(Record(queryDataSet.next(), tmpRecord.index))
            }

            //            if (curRecord == null || tmpRecord.record.getTimestamp != curRecord.record.getTimestamp ||
            //              !tmpRecord.record.getFields.get(0).deltaObjectId.equals(curRecord.record.getFields.get(0).deltaObjectId)) {
            if (curRecord == null || tmpRecord.record.getTimestamp != curRecord.record.getTimestamp ||
              !queryDataSet.getPaths.get(0).getDevice.equals(deltaObjectId)) {
              curRecord = tmpRecord
              deltaObjectId = queryDataSet.getPaths.get(0).getDevice //TODO
              val paths:util.List[org.apache.iotdb.tsfile.read.common.Path] = queryDataSet.getPaths
              //org.apache.iotdb.tsfile.read.common.Path
              paths.forEach(s => {
                measurementIds.add(s.getMeasurement) //TODO
              })

              hasNext = true
            }
          }

          hasNext
        }

        override def next(): InternalRow = {

          val fields = new scala.collection.mutable.HashMap[String, Field]()
          for (i <- 0 until curRecord.record.getFields.size()) {
            val field = curRecord.record.getFields.get(i)
            fields.put(measurementIds.get(i), field) //TODO
          }

          //index in one required row
          var index = 0
          requiredSchema.foreach((field: StructField) => {
            if (field.name == SQLConstant.RESERVED_TIME) {
              rowBuffer(index) = curRecord.record.getTimestamp
            } else if (field.name == SQLConstant.RESERVED_DELTA_OBJECT) {
              rowBuffer(index) = deltaObjectId
            } else if (DefaultSource.columnNames.contains(field.name)) {
              val columnIndex = DefaultSource.columnNames.indexOf(field.name)
              val columns = deltaObjectId.split(SQLConstant.REGEX_PATH_SEPARATOR)
              rowBuffer(index) = columns(columnIndex)
            } else {
              rowBuffer(index) = Converter.toSqlValue(fields.getOrElse(field.name, null))
            }
            index += 1
          })

          encoderForDataColumns.toRow(safeDataRow)
        }
      }
    }
  }

  override def shortName(): String = "tsfile"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    DefaultSource.columnNames.clear()

    //unfold delta_object
    if (options.contains(SQLConstant.DELTA_OBJECT_NAME)) {
      val columns = options(SQLConstant.DELTA_OBJECT_NAME).split(SQLConstant.REGEX_PATH_SEPARATOR)
      columns.foreach(f => {
        DefaultSource.columnNames += f
      })
    } else {
      //using delta_object
      DefaultSource.columnNames += SQLConstant.RESERVED_DELTA_OBJECT
    }

    new TsFileWriterFactory(options, DefaultSource.columnNames)
  }

  class TSFileDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

}


private[tsfile] object DefaultSource {
  val path = "path"
  val columnNames = new ArrayBuffer[String]()

  class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.defaultWriteObject()
      value.write(out)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      value = new Configuration(false)
      value.readFields(in)
    }
  }

}
