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

package org.apache.iotdb.db.qp.physical.crud;

import java.util.List;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.utils.Pair;

public class GroupByPlan extends AggregationPlan {

  private long unit;
  private long origin;
  private List<Pair<Long, Long>> intervals; // show intervals

  public GroupByPlan() {
    super();
    setOperatorType(Operator.OperatorType.GROUPBY);
  }

  public long getUnit() {
    return unit;
  }

  public void setUnit(long unit) {
    this.unit = unit;
  }

  public long getOrigin() {
    return origin;
  }

  public void setOrigin(long origin) {
    this.origin = origin;
  }

  public List<Pair<Long, Long>> getIntervals() {
    return intervals;
  }

  public void setIntervals(List<Pair<Long, Long>> intervals) {
    this.intervals = intervals;
  }
}
