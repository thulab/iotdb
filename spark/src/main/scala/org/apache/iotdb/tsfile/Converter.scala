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

package org.apache.iotdb.tsfile

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor
import org.apache.iotdb.tsfile.file.metadata.enums.{TSDataType, TSEncoding}
import org.apache.iotdb.tsfile.io.HDFSInput
import org.apache.iotdb.tsfile.qp.SQLConstant
import org.apache.iotdb.tsfile.read.TsFileSequenceReader
import org.apache.iotdb.tsfile.read.common.{Field, Path}
import org.apache.iotdb.tsfile.read.expression.impl.{BinaryExpression, GlobalTimeExpression, SingleSeriesExpression}
import org.apache.iotdb.tsfile.read.expression.{IExpression, QueryExpression}
import org.apache.iotdb.tsfile.read.filter.{TimeFilter, ValueFilter}
import org.apache.iotdb.tsfile.write.record.TSRecord
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint
import org.apache.iotdb.tsfile.write.schema.{FileSchema, MeasurementSchema, SchemaBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * This object contains methods that are used to convert schema and data between sparkSQL and TSFile.
  *
  */
object Converter {

  /**
    * Get union series in all tsfiles
    * e.g. (tsfile1:s1,s2) & (tsfile2:s2,s3) = s1,s2,s3
    *
    * @param files tsfiles
    * @param conf  hadoop configuration
    * @return union series
    */
  def getUnionSeries(files: Seq[FileStatus], conf: Configuration): util.ArrayList[MeasurementSchema] = {
    val unionSeries = new util.ArrayList[MeasurementSchema]()
    var seriesSet: mutable.Set[String] = mutable.Set()

    files.foreach(f => {
      val in = new HDFSInput(f.getPath, conf)
      //            val tsFileMetaData = TsFileMetadataUtils.getTsFileMetaData(f.getPath.toUri.toString)
      //TODO 不知这样直接读file不用那种HDFSInputStream其实好像也是file:///并没用hdfs://
      val reader = new TsFileSequenceReader(in)
      val tsFileMetaData = reader.readFileMetadata
      val devices = tsFileMetaData.getDeviceMap.keySet()
      val measurements = tsFileMetaData.getMeasurementSchema

      devices.foreach(d => {
        measurements.foreach(m => {
          val fullPath = d + "." + m._1
          if (!seriesSet.contains(fullPath)) {
            seriesSet += fullPath
            //            unionSeries.add(new MeasurementSchema(fullPath, s._2.getType, null.asInstanceOf[TSEncoding])
            unionSeries.add(new MeasurementSchema(fullPath, m._2.getType, m._2.getEncodingType)
            )
          }
        })
      })
    })

    unionSeries
  }

  /**
    * Get union series in all tsfiles
    * e.g. (tsfile1:s1,s2) & (tsfile2:s2,s3) = s1,s2,s3
    *
    * @param reader TsFile SequenceReader
    * @param conf   hadoop configuration
    * @return union series
    */
  def getUnionSeries(reader: TsFileSequenceReader, conf: Configuration): util.ArrayList[MeasurementSchema] = {
    val unionSeries = new util.ArrayList[MeasurementSchema]()
    var seriesSet: mutable.Set[String] = mutable.Set()

    val tsFileMetaData = reader.readFileMetadata
    val devices = tsFileMetaData.getDeviceMap.keySet()
    val measurements = tsFileMetaData.getMeasurementSchema

    devices.foreach(d => {
      measurements.foreach(m => {
        val fullPath = d + "." + m._1
        if (!seriesSet.contains(fullPath)) {
          seriesSet += fullPath
          //            unionSeries.add(new MeasurementSchema(fullPath, s._2.getType, null.asInstanceOf[TSEncoding])
          unionSeries.add(new MeasurementSchema(fullPath, m._2.getType, m._2.getEncodingType)
          )
        }
      })
    })

    unionSeries
  }

  /**
    * Convert TSFile columns to sparkSQL schema.
    *
    * @param tsfileSchema all time series information in TSFile
    * @return sparkSQL table schema
    */
  def toSqlSchema(tsfileSchema: util.ArrayList[MeasurementSchema]): StructType = {
    val fields = new ListBuffer[StructField]()
    fields += StructField(SQLConstant.RESERVED_TIME, LongType, nullable = false)

    tsfileSchema.foreach((series: MeasurementSchema) => {
      fields += StructField(series.getMeasurementId, series.getType match {
        case TSDataType.BOOLEAN => BooleanType
        case TSDataType.INT32 => IntegerType
        case TSDataType.INT64 => LongType
        case TSDataType.FLOAT => FloatType
        case TSDataType.DOUBLE => DoubleType
        case TSDataType.TEXT => StringType
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }, nullable = true)
    })

    SchemaType(StructType(fields.toList), nullable = false).dataType match {
      case t: StructType => t
      case _ => throw new RuntimeException(
        s"""TSFile schema cannot be converted to a Spark SQL StructType:
           |${tsfileSchema.toString}
           |""".stripMargin)
    }
  }

  /**
    * given a spark sql struct type, generate TsFile schema
    *
    * @param structType given sql schema
    * @return TsFile schema
    */
  def toTsFileSchema(structType: StructType, options: Map[String, String]): FileSchema = {
    val schemaBuilder = new SchemaBuilder()
    structType.fields.filter(f => {
      !SQLConstant.isReservedPath(f.name)
    }).foreach(f => {
      val seriesSchema = getSeriesSchema(f, options)
      schemaBuilder.addSeries(seriesSchema)
      //TODO 目前是会有重复且默认假设条件是同一个tsfile文件的同名sensor完全一样属性，不会因为不同device1.sensor1和device2.sensor1而sensor1不同属性
    })
    schemaBuilder.build()
  }

  /**
    * construct series schema from name and data type
    *
    * @param field   series name
    * @param options series data type
    * @return series schema
    */
  def getSeriesSchema(field: StructField, options: Map[String, String]): MeasurementSchema = {
    val conf = TSFileDescriptor.getInstance.getConfig
    val dataType = getTsDataType(field.dataType)
    val encodingStr = dataType match {
      case TSDataType.INT32 => options.getOrElse(SQLConstant.INT32, TSEncoding.RLE.toString)
      case TSDataType.INT64 => options.getOrElse(SQLConstant.INT64, TSEncoding.RLE.toString)
      case TSDataType.FLOAT => options.getOrElse(SQLConstant.FLOAT, TSEncoding.RLE.toString)
      case TSDataType.DOUBLE => options.getOrElse(SQLConstant.DOUBLE, TSEncoding.RLE.toString)
      case TSDataType.TEXT => options.getOrElse(SQLConstant.BYTE_ARRAY, TSEncoding.PLAIN.toString)
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
    val encoding = TSEncoding.valueOf(encodingStr)
    val fullPath = new Path(field.name)
    val measurement = fullPath.getMeasurement
    new MeasurementSchema(measurement, dataType, encoding)
  }

  def toQueryExpression(schema: StructType, filters: Seq[Filter]): QueryExpression = {
    //get selected paths from schema //TODO 自动的requiredSchema是会包含select和where后面的所有涉及项
    val paths = new util.ArrayList[org.apache.iotdb.tsfile.read.common.Path]
    schema.foreach(f => {
      if (!SQLConstant.isReservedPath(f.name)) {
        paths.add(new org.apache.iotdb.tsfile.read.common.Path(f.name))
      }
    })

    //remove invalid filters
    val validFilters = new ListBuffer[Filter]()
    filters.foreach { f => {
      if (isValidFilter(f))
        validFilters.add(f)
    }
    }
    if (validFilters.isEmpty) {
      val queryExpression = QueryExpression.create(paths, null)
      queryExpression
    } else {
      //construct filters to a binary tree
      var filterTree = validFilters.get(0)
      for (i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      val finalFilter = transformFilter(schema, filterTree)

      val queryExpression = QueryExpression.create(paths, finalFilter)
      queryExpression
    }
  }

  /**
    * Convert TSFile data to sparkSQL data.
    *
    * @param field one data point in TsFile
    * @return sparkSQL data
    */
  def toSqlValue(field: Field): Any = {
    if (field == null)
      return null
    if (field.isNull)
      null
    else field.getDataType match {
      case TSDataType.BOOLEAN => field.getBoolV
      case TSDataType.INT32 => field.getIntV
      case TSDataType.INT64 => field.getLongV
      case TSDataType.FLOAT => field.getFloatV
      case TSDataType.DOUBLE => field.getDoubleV
      case TSDataType.TEXT => field.getStringValue
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  /**
    * convert row to TSRecord
    *
    * @param row given spark sql row
    * @return TSRecord
    */
  def toTsRecord(row: Row): List[TSRecord] = {
    val schema = row.schema
    val time = row.getAs[Long](SQLConstant.RESERVED_TIME)
    //    val tsRecord = new TSRecord(time, delta_object)
    val deviceToRecord = scala.collection.mutable.Map[String, TSRecord]()
    schema.fields.filter(f => {
      !SQLConstant.isReservedPath(f.name)
    }).foreach(f => {
      val name = f.name
      val fullPath = new Path(name)
      val device = fullPath.getDevice
      val measurement = fullPath.getMeasurement
      if (!deviceToRecord.contains(device)) {
        deviceToRecord.put(device, new TSRecord(time, device))
      }
      val tsRecord: TSRecord = deviceToRecord.getOrElse(device, new TSRecord(time, device))
      //TODO 直接用get会返回的是option类型

      val dataType = getTsDataType(f.dataType)
      val index = row.fieldIndex(name)
      if (!row.isNullAt(index)) {
        val value = f.dataType match {
          case IntegerType => row.getAs[Int](name)
          case LongType => row.getAs[Long](name)
          case FloatType => row.getAs[Float](name)
          case DoubleType => row.getAs[Double](name)
          case StringType => row.getAs[String](name)
          case other => throw new UnsupportedOperationException(s"Unsupported type $other")
        }
        val dataPoint = DataPoint.getDataPoint(dataType, measurement, value.toString)
        tsRecord.addTuple(dataPoint)
      }
    })
    deviceToRecord.values.toList
  }

  /**
    * return the TsFile data type of given spark sql data type
    *
    * @param dataType spark sql data type
    * @return TsFile data type
    */
  def getTsDataType(dataType: DataType): TSDataType = {
    dataType match {
      case IntegerType => TSDataType.INT32
      case LongType => TSDataType.INT64
      case BooleanType => TSDataType.BOOLEAN
      case FloatType => TSDataType.FLOAT
      case DoubleType => TSDataType.DOUBLE
      case StringType => TSDataType.TEXT
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  private def isValidFilter(filter: Filter): Boolean = {
    filter match {
      case f: EqualTo => true
      case f: GreaterThan => true
      case f: GreaterThanOrEqual => true
      case f: LessThan => true
      case f: LessThanOrEqual => true
      case f: Or => isValidFilter(f.left) && isValidFilter(f.right)
      case f: And => isValidFilter(f.left) && isValidFilter(f.right)
      case f: Not => isValidFilter(f.child)
      case _ => false
    }
  }

  /**
    * Transform sparkSQL's filter binary tree to filter expression.
    *
    * @param requiredSchema to get relative columns' dataType information
    * @param node           filter tree's node
    * @return TSFile filter expression
    */
  private def transformFilter(requiredSchema: StructType, node: Filter): IExpression = {
    var filter: IExpression = null
    node match {
      case node: Not =>
        throw new Exception("unsupported NOT filter")
      //        val child = node.child
      //        val ltfilter = new SingleSeriesExpression(new Path(), ValueFilter.lt(node.value.toString))
      //
      //      // filter = BinaryExpression.or()

      case node: And =>
        filter = BinaryExpression.and(transformFilter(requiredSchema, node.left), transformFilter(requiredSchema, node.right))
        filter

      case node: Or =>
        filter = BinaryExpression.or(transformFilter(requiredSchema, node.left), transformFilter(requiredSchema, node.right))
        filter

      case node: EqualTo =>
        if (SQLConstant.isReservedPath(node.attribute.toLowerCase())) { //time
          filter = new GlobalTimeExpression(TimeFilter.eq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructEqFilter(requiredSchema, node.attribute, node.value)
        }
        filter
      case node: LessThan =>
        if (SQLConstant.isReservedPath(node.attribute.toLowerCase())) { //time
          filter = new GlobalTimeExpression(TimeFilter.lt(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructLtFilter(requiredSchema, node.attribute, node.value)
        }
        filter

      case node: LessThanOrEqual =>
        if (SQLConstant.isReservedPath(node.attribute.toLowerCase())) { //time
          filter = new GlobalTimeExpression(TimeFilter.ltEq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructLtEqFilter(requiredSchema, node.attribute, node.value)
        }
        filter

      case node: GreaterThan =>
        if (SQLConstant.isReservedPath(node.attribute.toLowerCase())) { //time
          filter = new GlobalTimeExpression(TimeFilter.gt(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructGtFilter(requiredSchema, node.attribute, node.value)
        }
        filter

      case node: GreaterThanOrEqual =>
        if (SQLConstant.isReservedPath(node.attribute.toLowerCase())) { //time
          filter = new GlobalTimeExpression(TimeFilter.gtEq(node.value.asInstanceOf[java.lang.Long]))
        } else {
          filter = constructGtEqFilter(requiredSchema, node.attribute, node.value)
        }
        filter

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
  }

  def constructEqFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      throw new Exception("requiredSchema does not contain nodeName:" + nodeName)
    }

    val dataType = requiredSchema.get(index).dataType

    dataType match {
      case IntegerType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Integer]))
        filter
      case LongType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Long]))
        filter
      case FloatType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Float]))
        filter
      case DoubleType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.eq(nodeValue.asInstanceOf[java.lang.Double]))
        filter
      case StringType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.eq(nodeValue.asInstanceOf[java.lang.String]))
        filter
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  def constructGtFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      throw new Exception("requiredSchema does not contain nodeName:" + nodeName)
    }
    val dataType = requiredSchema.get(index).dataType

    dataType match {
      case IntegerType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Integer]))
        filter
      case LongType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Long]))
        filter
      case FloatType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Float]))
        filter
      case DoubleType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gt(nodeValue.asInstanceOf[java.lang.Double]))
        filter
      case StringType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gt(nodeValue.asInstanceOf[java.lang.String]))
        filter
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  def constructGtEqFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      throw new Exception("requiredSchema does not contain nodeName:" + nodeName)
    }
    val dataType = requiredSchema.get(index).dataType

    dataType match {
      case IntegerType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Integer]))
        filter
      case LongType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Long]))
        filter
      case FloatType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Float]))
        filter
      case DoubleType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.Double]))
        filter
      case StringType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.gtEq(nodeValue.asInstanceOf[java.lang.String]))
        filter
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  def constructLtFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      throw new Exception("requiredSchema does not contain nodeName:" + nodeName)
    }
    val dataType = requiredSchema.get(index).dataType

    dataType match {
      case IntegerType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Integer]))
        filter
      case LongType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Long]))
        filter
      case FloatType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Float]))
        filter
      case DoubleType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.lt(nodeValue.asInstanceOf[java.lang.Double]))
        filter
      case StringType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.lt(nodeValue.asInstanceOf[java.lang.String]))
        filter
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  def constructLtEqFilter(requiredSchema: StructType, nodeName: String, nodeValue: Any): IExpression = {
    val fieldNames = requiredSchema.fieldNames
    val index = fieldNames.indexOf(nodeName)
    if (index == -1) {
      throw new Exception("requiredSchema does not contain nodeName:" + nodeName)
    }
    val dataType = requiredSchema.get(index).dataType

    dataType match {
      case IntegerType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Integer]))
        filter
      case LongType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Long]))
        filter
      case FloatType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Float]))
        filter
      case DoubleType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.Double]))
        filter
      case StringType =>
        val filter = new SingleSeriesExpression(new Path(nodeName),
          ValueFilter.ltEq(nodeValue.asInstanceOf[java.lang.String]))
        filter
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }

  class SparkSqlFilterException(message: String, cause: Throwable)
    extends Exception(message, cause) {
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)


}