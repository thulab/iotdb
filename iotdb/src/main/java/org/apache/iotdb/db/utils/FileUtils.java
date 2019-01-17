/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.utils;

import java.io.File;

/**
 * FileUtils is just used to calculate file attributes like file size, including some measurement
 * conversions among B, KB, MB etc.
 *
 * @author kangrong
 */
public class FileUtils {

  public static double getLocalFileByte(String filePath, Unit unit) {
    File f = new File(filePath);
    return getLocalFileByte(f, unit);
  }

  public static double getLocalFileByte(File file, Unit unit) {
    return format(transformUnit(file.length(), unit), 2);
  }

  /**
   * transform the byte value number to another unit.
   *
   * @param value
   *            - a number represented Byte which to be transformed
   * @param unit
   *            - the target unit to be transformed
   * @return - value number in unit of given parameter
   */
  public static double transformUnit(double value, Unit unit) {
    return value / Math.pow(1024, unit.ordinal());
  }

  /**
   * transform the value number from other unit to Byte unit.
   *
   * @param value
   *            - a number to be transformed
   * @param unit
   *            - the source unit to be transformed, maybe in unit of KB, MB, GB
   * @return - value number in unit of Byte
   */
  public static double transformUnitToByte(double value, Unit unit) {
    return value * Math.pow(1024, unit.ordinal());
  }

  /**
   * reserve some decimals for given double value.
   *
   * @param num
   *            - given double value
   * @param round
   *            - reserved decimal number
   * @return - double value in given decimal number
   */
  public static double format(double num, int round) {
    long a = (long) (num * Math.pow(10, round));
    return ((double) a) / Math.pow(10, round);
  }

  public static enum Unit {
    B, KB, MB, GB, TB, PB, EB
  }
}
