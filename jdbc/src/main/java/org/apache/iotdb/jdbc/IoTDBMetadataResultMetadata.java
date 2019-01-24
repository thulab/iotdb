/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/*
    TsfileMetadataResultMetadata implements a similar api like TsFileQueryResultSet
    to display column metadata.
 */

public class IoTDBMetadataResultMetadata implements ResultSetMetaData {

  private String[] showLabels;

  public IoTDBMetadataResultMetadata(String[] showLabels) {
    this.showLabels = showLabels;
  }

  @Override
  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCatalogName(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getColumnClassName(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnCount() throws SQLException {
    if (showLabels == null || showLabels.length == 0) {
      throw new SQLException("No column exists");
    }
    return showLabels.length;
  }

  @Override
  public int getColumnDisplaySize(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  private void checkColumnIndex(int column) throws SQLException {
    if (showLabels == null || showLabels.length == 0) {
      throw new SQLException("No column exists");
    }
    if (column > showLabels.length) {
      throw new SQLException(String.format("column %d does not exist", column));
    }
    if (column <= 0) {
      throw new SQLException("column index should start from 1");
    }
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    checkColumnIndex(column);
    return showLabels[column - 1];
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    // TODO Auto-generated method stub
    checkColumnIndex(column);

    // TEXT
    return Types.VARCHAR;
  }

  @Override
  public String getColumnTypeName(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getPrecision(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getScale(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getSchemaName(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTableName(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isAutoIncrement(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCaseSensitive(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCurrency(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int isNullable(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isReadOnly(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isSearchable(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isSigned(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isWritable(int arg0) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

}
