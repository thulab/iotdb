package cn.edu.tsinghua.iotdb.tool;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TsFileReader {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;

    private String path;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private long fileSize;
    private int fileMetadataSize;
    private TsFileMetaData fileMetaData;
    private List<Pair<Integer, Integer>> rowGroupBlockMetaDataSizeList;
    private List<TsRowGroupBlockMetaData> rowGroupBlockMetaDataList;
    private List<Pair<Integer, Integer>> rowGroupMetaDataSizeList;
    private List<RowGroupMetaData> rowGroupMetaDataList;
    private List<Pair<Integer, Pair<Integer, Integer>>> timeSeriesChunkMetaDataSizeList;
    private List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;
    private List<Pair<Integer, Integer>> pageSizeList;

    public TsFileReader(String path) throws IOException {
        this.path = path;
        this.randomAccessFileReader = new TsRandomAccessLocalFileReader(path);

        rowGroupBlockMetaDataList = new ArrayList<>();
        rowGroupBlockMetaDataSizeList = new ArrayList<>();
        rowGroupMetaDataList = new ArrayList<>();
        rowGroupMetaDataSizeList = new ArrayList<>();
        timeSeriesChunkMetaDataList = new ArrayList<>();
        timeSeriesChunkMetaDataSizeList = new ArrayList<>();
        pageSizeList = new ArrayList<>();
    }

    public void display() throws IOException {
        fileSize = randomAccessFileReader.length();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH);
        fileMetadataSize = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH - fileMetadataSize);
        byte[] buf = new byte[fileMetadataSize];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));

        int rgbCount = 0;
        for(Map.Entry<String, TsDeltaObject> entry : fileMetaData.getDeltaObjectMap().entrySet()) {
            TsDeltaObject deltaObject = entry.getValue();
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(
                    randomAccessFileReader,deltaObject.offset, deltaObject.metadataBlockSize));
            rowGroupBlockMetaDataSizeList.add(new Pair(deltaObject.metadataBlockSize, rowGroupBlockMetaData.getRowGroups().size()));

            for(RowGroupMetaData rowGroupMetaData : rowGroupBlockMetaData.getRowGroups()) {
                rowGroupMetaDataSizeList.add(new Pair(rowGroupMetaData.getTotalByteSize(), rowGroupMetaData.getTimeSeriesChunkMetaDataList().size()));

                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                    int size = (int) timeSeriesChunkMetaData.getTotalByteSize();
                    long offset = timeSeriesChunkMetaData.getProperties().getFileOffset();
                    byte[] bytes = new byte[size];
                    randomAccessFileReader.seek(offset);
                    randomAccessFileReader.read(bytes, 0, size);
                    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
                    int pageNum = 0;
                    while (inputStream.available() > 0) {
                        PageHeader header = ReadWriteThriftFormatUtils.readPageHeader(inputStream);
//                        inputStream.skip(header.compressed_page_size);
                        pageNum++;

                        Decoder valueDecoder = Decoder.getDecoderByType(
                                header.getData_page_header().getEncoding(), timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDataType());
                        Decoder defaultTimeDecoder = Decoder.getDecoderByType(Encoding.TS_2DIFF, TSDataType.INT64);
                        UnCompressor unCompressor = UnCompressor.getUnCompressor(timeSeriesChunkMetaData.getProperties().getCompression());
                        PageReader pageReader = constructPageReaderForNextPage(inputStream, header.getCompressed_page_size(), valueDecoder, defaultTimeDecoder,
                                timeSeriesChunkMetaData.getVInTimeSeriesChunkMetaData().getDataType(), unCompressor);
                        int rowNum = 0;
                        while(pageReader.hasNext()){
                            rowNum++;
                            pageReader.next();
                        }
                        pageSizeList.add(new Pair(header.compressed_page_size, rowNum));
                    }

                    timeSeriesChunkMetaDataSizeList.add(new Pair(timeSeriesChunkMetaData.getTotalByteSize(), new Pair(pageNum, timeSeriesChunkMetaData.getNumRows())));
                }
            }
            rgbCount++;
            System.out.println(rgbCount);
        }

        System.out.println(rowGroupBlockMetaDataSizeList.size());
        System.out.println(rowGroupMetaDataSizeList.size());
        System.out.println(timeSeriesChunkMetaDataSizeList.size());
        System.out.println(pageSizeList.size());
        writeIntoFile();
    }

    private void writeIntoFile() throws IOException {
        String prefix = "/Users/East/Desktop/results/";
        String suffix = ".txt";

        FileWriter rgbmdWriter = new FileWriter(prefix + "RGBMD" + suffix);
        FileWriter rgWriter = new FileWriter(prefix + "RG" + suffix);
        FileWriter tscWriter = new FileWriter(prefix + "TSC" + suffix);
        FileWriter pageWriter = new FileWriter(prefix + "PAGE" + suffix);

        for(Pair<Integer, Integer> pair : rowGroupBlockMetaDataSizeList){
            rgbmdWriter.write(pair.getKey() + "\t" + pair.getValue() + "\n");
        }
        for(Pair<Integer, Integer> pair : rowGroupMetaDataSizeList){
            rgWriter.write(pair.getKey() + "\t" + pair.getValue() + "\n");
        }
        int count = 0;
        for(Pair<Integer, Pair<Integer, Integer>> pair : timeSeriesChunkMetaDataSizeList){
            if(count % 100 == 0){
                tscWriter.write(pair.getKey() + "\t" + pair.getValue().getKey() + "\t" + pair.getValue().getValue() + "\n");
                count = 0;
            }
            count++;
        }
        for(Pair<Integer, Integer> pair : pageSizeList){
            if(count % 100 == 0){
                pageWriter.write(pair.getKey() + "\t" + pair.getValue() + "\n");
                count = 0;
            }
            count++;
        }

        rgbmdWriter.close();
        rgWriter.close();
        tscWriter.close();
        pageWriter.close();
    }

    private PageReader constructPageReaderForNextPage(InputStream inputStream, int compressedPageBodyLength, Decoder valueDecoder, Decoder timeDecoder, TSDataType dataType, UnCompressor unCompressor)
            throws IOException {
        byte[] compressedPageBody = new byte[compressedPageBodyLength];
        int readLength = inputStream.read(compressedPageBody, 0, compressedPageBodyLength);
        if (readLength != compressedPageBodyLength) {
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + compressedPageBody + ". Actual:" + readLength);
        }
        PageReader pageReader = new PageReader(new ByteArrayInputStream(unCompressor.uncompress(compressedPageBody)),
                dataType, valueDecoder, timeDecoder);
        return pageReader;
    }

    public static void main(String[] args) throws IOException {
        String path = "/Users/East/Desktop/1524261157000-1524291295414";
        TsFileReader reader = new TsFileReader(path);
        reader.display();
    }
}
