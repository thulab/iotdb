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
package org.apache.iotdb.db.query.component;

import org.junit.Ignore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Ignore
public class SimpleFileWriter {

    public static void writeFile(String path, byte[] bytes) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        FileOutputStream fileOutputStream = new FileOutputStream(path);
        fileOutputStream.write(bytes, 0, bytes.length);
        fileOutputStream.close();
    }

    public static void writeFile(int size, String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) (i % 200 + 1);
        }
        FileOutputStream fileOutputStream = new FileOutputStream(path);
        fileOutputStream.write(bytes, 0, size);
        fileOutputStream.close();
    }
}
