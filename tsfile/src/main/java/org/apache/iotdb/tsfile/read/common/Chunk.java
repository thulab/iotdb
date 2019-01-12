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
package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;

import java.nio.ByteBuffer;

/**
 * used in query
 */
public class Chunk {

    private ChunkHeader chunkHeader;
    private ByteBuffer chunkData;

    public Chunk(ChunkHeader header, ByteBuffer buffer) {
        this.chunkHeader = header;
        this.chunkData = buffer;
    }

    public ChunkHeader getHeader() {
        return chunkHeader;
    }

    public ByteBuffer getData() {
        return chunkData;
    }
}
