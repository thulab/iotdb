/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.query.dataset;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;

public abstract class QueryDataSet {

    protected List<Path> paths;
    protected List<TSDataType> dataTypes;

    public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes) {
        this.paths = paths;
        this.dataTypes = dataTypes;
    }

    /**
     * This method is used for batch query.
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * This method is used for batch query, return RowRecord.
     */
    public abstract RowRecord next() throws IOException;

    public List<Path> getPaths() {
        return paths;
    }

    public List<TSDataType> getDataTypes() {
        return dataTypes;
    }

}
