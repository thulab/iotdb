package cn.edu.tsinghua.iotdb.engine.deserialization;

import cn.edu.tsinghua.iotdb.engine.overflow.io.OverflowReadWriter;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFFileMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFRowGroupListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.metadata.OFSeriesListMetadata;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowReadWriteThriftFormatUtils;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * An Overflow FileDeserialization
 */
public class OverflowFileDeserialization {

    private static final Logger LOGGER = LoggerFactory.getLogger(OverflowFileDeserialization.class);

    private String fileName;
    private String restoreFileName;

    public OverflowFileDeserialization(String fileName, String restoreFileName) {
        this.fileName = fileName;
        this.restoreFileName = restoreFileName;
    }

    public static void main(String[] args) throws IOException {
        String fileName = "overflow/root.dt.wf632815.type4.overflow";
        String restoreFileName = "overflow/root.dt.wf632815.type4.overflow.restore";

        OverflowFileDeserialization deserialization = new OverflowFileDeserialization(fileName, restoreFileName);
        System.out.println(deserialization.getOverflowRowNumbers());
//        OverflowFileIO io = new OverflowFileIO(overflowReadWriter, "", struct.lastOverflowRowGroupPosition);
//        Map<String, Map<String, List<TimeSeriesChunkMetaData>>> ans = io.getSeriesListMap();
//
//        for (Map.Entry<String, Map<String, List<TimeSeriesChunkMetaData>>> entry : ans.entrySet()) {
//            String deltaObjectId = entry.getKey();
//            System.out.println(deltaObjectId);
//            //OverflowSeriesImpl overflowSeriesImpl = new OverflowSeriesImpl();
//        }
    }

    private long getOverflowRowNumbers() throws IOException {
        OverflowReadWriter overflowReadWriter = new OverflowReadWriter(fileName);
        OverflowStoreStruct struct = getLastPos(restoreFileName);

        long rowNumber = 0;
        for (OFRowGroupListMetadata rowGroupMetadata : struct.ofFileMetadata.getRowGroupLists()) {
            //System.out.println("deltaObjectId : " + rowGroupMetadata.getDeltaObjectId());
            for (OFSeriesListMetadata oFSeriesListMetadata: rowGroupMetadata.getSeriesLists()) {
                //System.out.println("measurementId : " + oFSeriesListMetadata.getMeasurementId());
                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : oFSeriesListMetadata.getMetaDatas())
                    rowNumber += timeSeriesChunkMetaData.getNumRows();
                    //System.out.println(timeSeriesChunkMetaData.toString());
            }
            //System.out.println();
        }

        return rowNumber;
    }

    private OverflowStoreStruct getLastPos(String overflowRetoreFilePath) {
        synchronized (overflowRetoreFilePath) {

            File overflowRestoreFile = new File(overflowRetoreFilePath);
            if (!overflowRestoreFile.exists()) {
                LOGGER.error("file not exist");
            }
            byte[] buff = new byte[8];
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(overflowRestoreFile);
            } catch (FileNotFoundException e) {
                LOGGER.error("The overflow restore file is not found, the file path is {}", overflowRetoreFilePath);
            }
            int off = 0;
            int len = buff.length - off;
            cn.edu.tsinghua.iotdb.engine.overflow.thrift.OFFileMetadata thriftfileMetadata = null;
            try {
                do {
                    int num = fileInputStream.read(buff, off, len);
                    off = off + num;
                    len = len - num;
                } while (len > 0);
                long lastOverflowFilePosition = BytesUtils.bytesToLong(buff);

                if (lastOverflowFilePosition != -1) {
                    return new OverflowStoreStruct(lastOverflowFilePosition, -1, null);
                }

                off = 0;
                len = buff.length - off;
                do {
                    int num = fileInputStream.read(buff, off, len);
                    off = off + num;
                    len = len - num;
                } while (len > 0);

                long lastOverflowRowGroupPosition = BytesUtils.bytesToLong(buff);
                thriftfileMetadata = OverflowReadWriteThriftFormatUtils.readOFFileMetaData(fileInputStream);
                TSFileMetaDataConverter metadataConverter = new TSFileMetaDataConverter();
                OFFileMetadata ofFileMetadata = metadataConverter.toOFFileMetadata(thriftfileMetadata);
                return new OverflowStoreStruct(lastOverflowFilePosition, lastOverflowRowGroupPosition, ofFileMetadata);
            } catch (IOException e) {
                LOGGER.error(
                        "Read the data: lastOverflowFilePostion, lastOverflowRowGroupPostion, offilemetadata error");
            } finally {
                if (fileInputStream != null) {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return null;
    }

    private class OverflowStoreStruct {
        public final long lastOverflowFilePosition;
        public final long lastOverflowRowGroupPosition;
        public final OFFileMetadata ofFileMetadata;

        public OverflowStoreStruct(long lastOverflowFilePosition, long lastOverflowRowGroupPosition,
                                   OFFileMetadata ofFileMetadata) {
            super();
            this.lastOverflowFilePosition = lastOverflowFilePosition;
            this.lastOverflowRowGroupPosition = lastOverflowRowGroupPosition;
            this.ofFileMetadata = ofFileMetadata;
        }
    }
}
