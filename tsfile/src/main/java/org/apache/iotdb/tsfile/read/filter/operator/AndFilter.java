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
package org.apache.iotdb.tsfile.read.filter.operator;

import org.apache.iotdb.tsfile.read.filter.DigestForFilter;
import org.apache.iotdb.tsfile.read.filter.basic.BinaryFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Both the left and right operators of AndExpression must satisfy the condition.
 */
public class AndFilter extends BinaryFilter {

  private static final long serialVersionUID = 6705254093824897938L;

  public AndFilter(Filter left, Filter right) {
    super(left, right);
  }

  @Override
  public boolean satisfy(DigestForFilter digest) {
    return left.satisfy(digest) && right.satisfy(digest);
  }

  @Override
  public boolean satisfy(long time, Object value) {
    return left.satisfy(time, value) && right.satisfy(time, value);
  }

  @Override
  public boolean satisfyStartEndTime(long startTime, long endTime) {
    return left.satisfyStartEndTime(startTime, endTime) && right
        .satisfyStartEndTime(startTime, endTime);
  }

  @Override
  public String toString() {
    return "(" + left + " && " + right + ")";
  }
}
