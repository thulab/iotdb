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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.bufferwrite.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadataIndex;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.BytesUtils;

public class TsFileChecker {

  public static int checkTsFile(String path) {
    TsFileSequenceReader reader = null;
    long lastPosition = Long.MAX_VALUE;
    try {
      boolean complete = true;
      String restoreFilePath = path + RestorableTsFileIOWriter.getRestoreSuffix();
      long tsFileLen = new File(path).length();
      try {
        lastPosition = readLastPositionFromRestoreFile(restoreFilePath);
        complete = false;
      } catch (IOException ex) {
        lastPosition = tsFileLen;
        complete = true;
      }

      if (lastPosition > tsFileLen) {
        return 0;
      }
      if (lastPosition <= TSFileConfig.MAGIC_STRING.length()) {
        return 2;
      }

      reader = new TsFileSequenceReader(path, complete);
      if (!complete) {
        reader.position(TSFileConfig.MAGIC_STRING.length());
      }
      reader.readHeadMagic();
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        if (reader.position() > lastPosition) {
          return 3;
        }

        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader();
            Decoder defaultTimeDecoder = Decoder.getDecoderByType(
                TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder),
                TSDataType.INT64);
            Decoder valueDecoder = Decoder
                .getDecoderByType(header.getEncodingType(), header.getDataType());
            for (int j = 0; j < header.getNumOfPages(); j++) {
              PageHeader pageHeader = reader.readPageHeader(header.getDataType());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              PageReader reader1 = new PageReader(pageData, header.getDataType(), valueDecoder,
                  defaultTimeDecoder);
              while (reader1.hasNextBatch()) {
                BatchData batchData = reader1.nextBatch();
                while (batchData.hasNext()) {
                  batchData.currentTime();
                  batchData.currentValue();
                  batchData.next();
                }
              }
            }
            break;
          case MetaMarker.CHUNK_GROUP_FOOTER:
            reader.readChunkGroupFooter();
            if (reader.position() == lastPosition) {
              return 2;
            }
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      TsFileMetaData metaData = reader.readFileMetadata();
      List<TsDeviceMetadataIndex> deviceMetadataIndexList = metaData.getDeviceMap().values().stream()
          .sorted((x, y) -> (int) (x.getOffset() - y.getOffset())).collect(Collectors.toList());
      for (TsDeviceMetadataIndex index : deviceMetadataIndexList) {
        TsDeviceMetadata deviceMetadata = reader.readTsDeviceMetaData(index);
        deviceMetadata.getChunkGroupMetaDataList();
      }
      reader.readTailMagic();
      reader.close();

      return 1;
    } catch (Exception ex) {
      System.out.println(ex);
      try {
        if (reader != null && reader.position() > lastPosition) {
          return 3;
        } else {
          return 0;
        }
      } catch (IOException e) {
        return 0;
      }
    }
  }

  private static long readLastPositionFromRestoreFile(String path) throws IOException {
    int tsfilePositionByteSize = RestorableTsFileIOWriter.getTsPositionByteSize();
    byte[] lastPostionBytes = new byte[tsfilePositionByteSize];
    RandomAccessFile randomAccessFile = null;
    randomAccessFile = new RandomAccessFile(path, "r");

    long fileLength = randomAccessFile.length();
    randomAccessFile.seek(fileLength - tsfilePositionByteSize);
    randomAccessFile.read(lastPostionBytes);
    long lastPosition = BytesUtils.bytesToLong(lastPostionBytes);
    randomAccessFile.close();
    return lastPosition;
  }

  public static List<File> getFileList(String dirPath) {
    List<File> res = new ArrayList<>();
    File dir = new File(dirPath);
    if (dir.isFile()) {
      res.add(dir);
      return res;
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          res.addAll(getFileList(files[i].getAbsolutePath()));
        } else {
          res.add(files[i]);
        }
      }

    }
    return res;
  }

  public static List<File> getLastDirList(String dirPath) {
    List<File> res = new ArrayList<>();
    File dir = new File(dirPath);
    if (dir.isFile()) {
      return Collections.emptyList();
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          res.addAll(getLastDirList(files[i].getAbsolutePath()));
        } else {
          res.add(dir);
          return res;
        }
      }

    }
    return res;
  }

  public static int getRestoreFileNum(File dir) throws Exception {
    int count = 0;
    if (dir.isFile()) {
      throw new Exception("Path: " + dir.getAbsolutePath() + " is a file!");
    }

    File[] files = dir.listFiles();
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        if (files[i].isFile() && files[i].getPath().endsWith(RestorableTsFileIOWriter.getRestoreSuffix())) {
          count++;
        }
      }

    }
    return count;
  }

  public static void main(String[] args) throws Exception {
//    args = new String[]{"/Users/East/Desktop/长测TsFile分析/tsfile"};

    if (args == null || args.length < 1) {
      System.out.println("Please input root dir!");
      System.exit(1);
    }

    String root = args[0];

    List<File> lastDirList = TsFileChecker.getLastDirList(root);
    List<String> wrongRestoreNumDirList = new ArrayList<>();
    for (int i = 0; i < lastDirList.size(); i++) {
      int count = TsFileChecker.getRestoreFileNum(lastDirList.get(i));
      if (count > 1) {
        wrongRestoreNumDirList.add(lastDirList.get(i).getAbsolutePath());
      }
    }
    System.out.println("Directories of more than one restore file:");
    for (int i = 0; i < wrongRestoreNumDirList.size(); i++) {
      System.out.println(wrongRestoreNumDirList.get(i));
    }
    System.out.println();

    List<File> fileList = TsFileChecker.getFileList(root);
    Map<Integer, List<String>> tsfileStatusMap = new HashMap<>();
    tsfileStatusMap.put(0, new ArrayList<>());
    tsfileStatusMap.put(1, new ArrayList<>());
    tsfileStatusMap.put(2, new ArrayList<>());
    tsfileStatusMap.put(3, new ArrayList<>());
    int num = fileList.size();
    System.out.println("Num of file: " + num);
    for (int i = 0; i < fileList.size(); i++) {
      String filePath = fileList.get(i).getAbsolutePath();
      System.out.println("Check No." + (i + 1) + ": " + filePath);
      if (filePath.endsWith(RestorableTsFileIOWriter.getRestoreSuffix())) {
        num--;
        continue;
      }
      int status = TsFileChecker.checkTsFile(filePath);
      System.out.println(status);
      tsfileStatusMap.get(status).add(filePath);
    }
    System.out.println("Num of TsFile: " + num);
    System.out.println("TsFile status:");
    for (Entry<Integer, List<String>> integerListEntry : tsfileStatusMap.entrySet()) {
      System.out.println(integerListEntry.getKey() + ": " + integerListEntry.getValue().size());
//      integerListEntry.getValue().forEach(value -> System.out.println(value));
    }
  }
}
