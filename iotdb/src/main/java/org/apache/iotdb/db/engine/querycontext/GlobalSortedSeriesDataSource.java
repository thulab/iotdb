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
package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.engine.filenode.IntervalFileNode;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public class GlobalSortedSeriesDataSource {
    private Path seriesPath;

    // sealed tsfile
    private List<IntervalFileNode> sealedTsFiles;

    // unsealed tsfile
    private UnsealedTsFile unsealedTsFile;

    // seq mem-table
    private TimeValuePairSorter readableChunk;

    public GlobalSortedSeriesDataSource(Path seriesPath, List<IntervalFileNode> sealedTsFiles,
            UnsealedTsFile unsealedTsFile, TimeValuePairSorter readableChunk) {
        this.seriesPath = seriesPath;
        this.sealedTsFiles = sealedTsFiles;
        this.unsealedTsFile = unsealedTsFile;

        this.readableChunk = readableChunk;
    }

    public boolean hasSealedTsFiles() {
        return sealedTsFiles != null && sealedTsFiles.size() > 0;
    }

    public List<IntervalFileNode> getSealedTsFiles() {
        return sealedTsFiles;
    }

    public boolean hasUnsealedTsFile() {
        return unsealedTsFile != null;
    }

    public UnsealedTsFile getUnsealedTsFile() {
        return unsealedTsFile;
    }

    public boolean hasRawSeriesChunk() {
        return readableChunk != null;
    }

    public TimeValuePairSorter getReadableChunk() {
        return readableChunk;
    }

    public void setSealedTsFiles(List<IntervalFileNode> sealedTsFiles) {
        this.sealedTsFiles = sealedTsFiles;
    }

    public void setUnsealedTsFile(UnsealedTsFile unsealedTsFile) {
        this.unsealedTsFile = unsealedTsFile;
    }

    public void setReadableChunk(TimeValuePairSorter readableChunk) {
        this.readableChunk = readableChunk;
    }

    public void setSeriesPath(Path seriesPath) {
        this.seriesPath = seriesPath;
    }

    public Path getSeriesPath() {
        return seriesPath;
    }

}
