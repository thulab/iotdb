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
package org.apache.iotdb.db.utils;

import java.io.Serializable;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

public abstract class TsPrimitiveType implements Serializable {

  /**
   * get tsPrimitiveType by dataType.
   *
   * @param dataType -given TsDataType
   * @param v -
   */
  public static TsPrimitiveType getByType(TSDataType dataType, Object v) {
    switch (dataType) {
      case BOOLEAN:
        return new TsPrimitiveType.TsBoolean((boolean) v);
      case INT32:
        return new TsPrimitiveType.TsInt((int) v);
      case INT64:
        return new TsPrimitiveType.TsLong((long) v);
      case FLOAT:
        return new TsPrimitiveType.TsFloat((float) v);
      case DOUBLE:
        return new TsPrimitiveType.TsDouble((double) v);
      case TEXT:
        return new TsPrimitiveType.TsBinary((Binary) v);
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  public boolean getBoolean() {
    throw new UnsupportedOperationException("getBoolean() is not supported for current sub-class");
  }

  public int getInt() {
    throw new UnsupportedOperationException("getInt() is not supported for current sub-class");
  }

  public long getLong() {
    throw new UnsupportedOperationException("getLong() is not supported for current sub-class");
  }

  public float getFloat() {
    throw new UnsupportedOperationException("getFloat() is not supported for current sub-class");
  }

  public double getDouble() {
    throw new UnsupportedOperationException("getDouble() is not supported for current sub-class");
  }

  public Binary getBinary() {
    throw new UnsupportedOperationException("getBinary() is not supported for current sub-class");
  }

  /**
   * get the size of one instance of current class.
   *
   * @return size of one instance of current class
   */
  public abstract int getSize();

  public abstract Object getValue();

  public abstract String getStringValue();

  public abstract TSDataType getDataType();

  @Override
  public String toString() {
    return getStringValue();
  }

  @Override
  public boolean equals(Object object) {
    return (object instanceof TsPrimitiveType) && (((TsPrimitiveType) object).getValue()
        .equals(getValue()));
  }

  @Override
  public int hashCode(){
    return getValue().hashCode();
  }

  public static class TsBoolean extends TsPrimitiveType {

    private boolean value;

    public TsBoolean(boolean value) {
      this.value = value;
    }

    @Override
    public boolean getBoolean() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 1;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.BOOLEAN;
    }
  }

  public static class TsInt extends TsPrimitiveType {

    private int value;

    public TsInt(int value) {
      this.value = value;
    }

    @Override
    public int getInt() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.INT32;
    }
  }

  public static class TsLong extends TsPrimitiveType {

    private long value;

    public TsLong(long value) {
      this.value = value;
    }

    @Override
    public long getLong() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.INT64;
    }

    @Override
    public Object getValue() {
      return value;
    }
  }

  public static class TsFloat extends TsPrimitiveType {

    private float value;

    public TsFloat(float value) {
      this.value = value;
    }

    @Override
    public float getFloat() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 4;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.FLOAT;
    }
  }

  public static class TsDouble extends TsPrimitiveType {

    private double value;

    public TsDouble(double value) {
      this.value = value;
    }

    @Override
    public double getDouble() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 8;
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.DOUBLE;
    }
  }

  public static class TsBinary extends TsPrimitiveType {

    private Binary value;

    public TsBinary(Binary value) {
      this.value = value;
    }

    @Override
    public Binary getBinary() {
      return value;
    }

    @Override
    public int getSize() {
      return 4 + 4 + value.getLength();
    }

    @Override
    public Object getValue() {
      return value;
    }

    @Override
    public String getStringValue() {
      return String.valueOf(value);
    }

    @Override
    public TSDataType getDataType() {
      return TSDataType.TEXT;
    }
  }

  public static void main(String[] args){
    TsPrimitiveType type1 = new TsBoolean(false);
    TsPrimitiveType type2 = new TsBoolean(true);
    TsPrimitiveType type3 = new TsBoolean(false);
    TsPrimitiveType type4 = new TsDouble(1);

    System.out.println(type1.equals(type2));
    System.out.println(type1.equals(type3));
    System.out.println(type1.equals(type4));
  }
}