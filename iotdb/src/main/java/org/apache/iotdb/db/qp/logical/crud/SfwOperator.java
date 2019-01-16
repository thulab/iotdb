/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.iotdb.db.qp.logical.crud;

import java.util.List;
import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * SfwOperator(select-from-where) includes four subclass: INSERT,DELETE,UPDATE,QUERY. All of these
 * four statements has three partition: select clause, from clause and filter clause(where clause).
 */
public abstract class SfwOperator extends RootOperator {

  private SelectOperator selectOperator;
  private FromOperator fromOperator;
  private FilterOperator filterOperator;
  private boolean hasAggregation = false;

  public SfwOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.SFW;
  }

  /**
   * set selectOperator, then init hasAggregation according to selectOperator.
   * */
  public void setSelectOperator(SelectOperator sel) {
    this.selectOperator = sel;
    if (!sel.getAggregations().isEmpty()) {
      hasAggregation = true;
    }
  }

  public void setFromOperator(FromOperator from) {
    this.fromOperator = from;
  }

  public void setFilterOperator(FilterOperator filter) {
    this.filterOperator = filter;
  }

  public FromOperator getFromOperator() {
    return fromOperator;
  }

  public SelectOperator getSelectOperator() {
    return selectOperator;
  }

  public FilterOperator getFilterOperator() {
    return filterOperator;
  }

  /**
   * get information from SelectOperator and FromOperator and generate all table paths.
   *
   * @return - a list of seriesPath
   */
  public List<Path> getSelectedPaths() {
    List<Path> suffixPaths = null;
    if (selectOperator != null) {
      suffixPaths = selectOperator.getSuffixPaths();
    }
    return suffixPaths;
  }

  public boolean hasAggregation() {
    return hasAggregation;
  }
}
