package cn.edu.thu.tsfile

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object spark {

  /**
    * add a method to DataFrameReader
    */
  implicit class TSFileDataFrameReader(reader: DataFrameReader) {
    def tsfile: String => DataFrame = reader.format("cn.edu.thu.tsfile.spark").load
  }
}
