package cn.edu.tsinghua.iotdb.tool;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import org.apache.commons.io.Charsets;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * @author East
 */
public class UIDTest {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    private String tsFilePath;
    private ITsRandomAccessFileReader randomAccessFileReader;

    private List<String> deltaobjectIdList;
    private List<String> measurementIdList;

    MessageDigest md5Digest;

    public UIDTest(String tsFilePath) throws IOException, NoSuchAlgorithmException {
        this.tsFilePath = tsFilePath;

        deltaobjectIdList = new ArrayList<>();
        measurementIdList = new ArrayList<>();

        md5Digest = MessageDigest.getInstance("MD5");
    }

    public void analyze() throws IOException {
        this.randomAccessFileReader = new TsRandomAccessLocalFileReader(tsFilePath);

        long fileSize = randomAccessFileReader.length();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetadataSize = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH - fileMetadataSize);
        byte[] buf = new byte[fileMetadataSize];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        TsFileMetaData fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));
        deltaobjectIdList.addAll(fileMetaData.getDeltaObjectMap().keySet());
        for(TimeSeriesMetadata timeSeriesMetadata : fileMetaData.getTimeSeriesList()){
            measurementIdList.add(timeSeriesMetadata.getMeasurementUID());
        }
    }

    private long UID(String value) {
        byte[] md5hash;
        synchronized (md5Digest) {
            md5hash = md5Digest.digest(value.getBytes());
        }
        long hash = 0L;
        for (int i = 0; i < 8; i++) {
            hash = hash << 8 | md5hash[i] & 0x00000000000000FFL;
        }
        return hash;
    }

    private long bytesToLong(byte[] bytes){
        long res = 0L;
        for (int i = 0; i < bytes.length; i++) {
            res = res << 8 | bytes[i] & 0x00000000000000FFL;
        }
        return res;
    }

    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        String path = "/Users/East/Desktop/tsfile解析/data/1524261157000-1524291295414";
        UIDTest test = new UIDTest(path);
        test.analyze();

        Set<Long> uidSet = new HashSet<>();
        for(String deltaobjectId : test.deltaobjectIdList) {
            long uid = test.UID(deltaobjectId);
            uidSet.add(uid);
        }
        System.out.println(test.deltaobjectIdList.size());
        System.out.println(uidSet.size());

        uidSet.clear();
        for(String measurementId : test.measurementIdList) {
            long uid = test.UID(measurementId);
            if(uidSet.contains(uid)) {
                System.out.println("warn:" + measurementId);
                System.out.println(uid);
            }
            uidSet.add(uid);
        }
        System.out.println(test.measurementIdList.size());
        System.out.println(uidSet.size());
    }
}
