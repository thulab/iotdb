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
package org.apache.iotdb.tsfile.read.reader.series;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 * Series reader is used to query one series of one tsfile.
 */
public abstract class FileSeriesReader {

    protected ChunkLoader chunkLoader;
    protected List<ChunkMetaData> chunkMetaDataList;
    protected ChunkReader chunkReader;
    private int chunkToRead;

    private BatchData data;

    public FileSeriesReader(ChunkLoader chunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        this.chunkLoader = chunkLoader;
        this.chunkMetaDataList = chunkMetaDataList;
        this.chunkToRead = 0;
    }

    public boolean hasNextBatch() {

        // current chunk has data
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            return true;

            // has additional chunk to read
        } else {
            return chunkToRead < chunkMetaDataList.size();
        }

    }

    public BatchData nextBatch() throws IOException {

        // current chunk has additional batch
        if (chunkReader != null && chunkReader.hasNextBatch()) {
            data = chunkReader.nextBatch();
            return data;
        }

        // current chunk does not have additional batch, init new chunk reader
        while (chunkToRead < chunkMetaDataList.size()) {

            ChunkMetaData chunkMetaData = chunkMetaDataList.get(chunkToRead++);
            if (chunkSatisfied(chunkMetaData)) {
                // chunk metadata satisfy the condition
                initChunkReader(chunkMetaData);

                if (chunkReader.hasNextBatch()) {
                    data = chunkReader.nextBatch();
                    return data;
                }
            }
        }

        if (data == null)
            return new BatchData();
        return data;
    }

    public BatchData currentBatch() {
        return data;
    }

    protected abstract void initChunkReader(ChunkMetaData chunkMetaData) throws IOException;

    protected abstract boolean chunkSatisfied(ChunkMetaData chunkMetaData);

    public void close() throws IOException {
        chunkLoader.close();
    }

}
