package cn.edu.thu

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object tsfile {

  /**
    * add a method to DataFrameReader
    */
  implicit class TSFileDataFrameReader(reader: DataFrameReader) {
    def tsfile: String => DataFrame = reader.format("cn.edu.thu.tsfile").load
  }
}
