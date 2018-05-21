package cn.edu.tsinghua.iotdb.tool;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.Encoding;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import javafx.util.Pair;

import java.io.*;
import java.util.*;

public class TsFileAnalyzer {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final double SCALE = 0.05;
    private static final int BOX_NUM = 20;

    private String tsFilePath;
    private ITsRandomAccessFileReader randomAccessFileReader;
    private long fileSize;
    private int fileMetadataSize;
    private TsFileMetaData fileMetaData;
    private List<Integer> rowGroupBlockMetaDataSizeList;
    private List<Integer> rowGroupBlockMetaDataContentList;
    private List<Integer> rowGroupMetaDataSizeList;
    private List<Integer> rowGroupMetaDataContentList;
    private List<Integer> timeSeriesChunkMetaDataSizeList;
    private List<Integer> timeSeriesChunkMetaDataContentList;
    private List<Integer> timeSeriesChunkMetaDataRowNumList;
    private List<Integer> pageSizeList;
    private List<Integer> pageContentList;

    private int mainIndex = 0;
    private int subIndex = 0;
    private FileWriter outputWriter;

    public TsFileAnalyzer(String tsFilePath) throws IOException {
        this.tsFilePath = tsFilePath;
        this.randomAccessFileReader = new TsRandomAccessLocalFileReader(tsFilePath);

        rowGroupBlockMetaDataSizeList = new ArrayList<>();
        rowGroupBlockMetaDataContentList = new ArrayList<>();
        rowGroupMetaDataSizeList = new ArrayList<>();
        rowGroupMetaDataContentList = new ArrayList<>();
        timeSeriesChunkMetaDataSizeList = new ArrayList<>();
        timeSeriesChunkMetaDataContentList = new ArrayList<>();
        timeSeriesChunkMetaDataRowNumList = new ArrayList<>();
        pageSizeList = new ArrayList<>();
        pageContentList = new ArrayList<>();
    }

    public void analyze() throws IOException {
        fileSize = randomAccessFileReader.length();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH);
        fileMetadataSize = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH - fileMetadataSize);
        byte[] buf = new byte[fileMetadataSize];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));

        int rgbCount = 0;
        System.out.println(fileMetaData.getDeltaObjectMap().size());
        for(Map.Entry<String, TsDeltaObject> entry : fileMetaData.getDeltaObjectMap().entrySet()) {
            TsDeltaObject deltaObject = entry.getValue();
            TsRowGroupBlockMetaData rowGroupBlockMetaData = new TsRowGroupBlockMetaData();
            rowGroupBlockMetaData.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(
                    randomAccessFileReader,deltaObject.offset, deltaObject.metadataBlockSize));
            rowGroupBlockMetaDataSizeList.add(deltaObject.metadataBlockSize);
            rowGroupBlockMetaDataContentList.add(rowGroupBlockMetaData.getRowGroups().size());

            for(RowGroupMetaData rowGroupMetaData : rowGroupBlockMetaData.getRowGroups()) {
                rowGroupMetaDataSizeList.add((int)rowGroupMetaData.getTotalByteSize());
                rowGroupMetaDataContentList.add(rowGroupMetaData.getTimeSeriesChunkMetaDataList().size());

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
                        pageSizeList.add(header.compressed_page_size);
                        pageContentList.add(rowNum);
                    }

                    timeSeriesChunkMetaDataSizeList.add(size);
                    timeSeriesChunkMetaDataContentList.add(pageNum);
                    timeSeriesChunkMetaDataRowNumList.add((int)timeSeriesChunkMetaData.getNumRows());
                }
            }
            rgbCount++;
            System.out.println(rgbCount);
        }
    }

    private void writeMainTitle(String content) throws IOException {
        mainIndex++;
        subIndex = 0;
        outputWriter.write("\n");
        outputWriter.write(mainIndex + "." + content + "\n");
    }

    private void writeSubTitle(String content) throws IOException {
        subIndex++;
        outputWriter.write(mainIndex + "." + subIndex + "." + content + "\n");
    }

    private void writeContent(String content) throws IOException {
        outputWriter.write("\t" + content + "\n");
    }

    private void writeOneBox(int start, int end, int num, float rate) throws IOException {
        writeContent("\t" + start + " ~ " + (end - 1) + "（" + (end - start) + "）: " + num + "，占" + rate * 100 + "%");
    }

    private void writeOneBox(int key, int num, float rate) throws IOException {
        writeContent("\t" + key + ": " + num + "，" + rate * 100 + "%");
    }

    private List<Long> getSimpleStatistics(List<Integer> dataList){
        long min, max, sum;
        min = dataList.get(0);
        max = dataList.get(0);
        sum = 0;

        for(int data : dataList){
            if(min > data)min = data;
            if(max < data)max = data;
            sum += data;
        }

        List<Long> res = new ArrayList<>();
        res.add(min);
        res.add(max);
        res.add(sum);
        return res;
    }

    private void writeDistribution(List<Integer> dataList) throws IOException {
        Collections.sort(dataList);

        int scalesize = (int) (dataList.size() * SCALE);
        int min  = dataList.get(0);
        int max = dataList.get(dataList.size() - 1);
        int start = dataList.get(scalesize);
        int end = dataList.get(dataList.size() - scalesize);
        int shift = (end - start) / BOX_NUM;
        if(shift <= 0)shift = 1;

        if(max - min < BOX_NUM){
            Map<Integer, Integer> boxes = new HashMap<>();

            int count = 0;
            int lastdata = dataList.get(0);
            for(int data : dataList){
                if(lastdata == data)count++;
                else{
                    boxes.put(lastdata, count);
                    lastdata = data;
                    count = 1;
                }
            }
            boxes.put(lastdata, count);

            for(int i = min;i <= max;i++){
                if(boxes.containsKey(i))writeOneBox(i, boxes.get(i), boxes.get(i) / (float)dataList.size());
            }

            return;
        }

        writeOneBox(min, start, scalesize, scalesize / (float)dataList.size());
        int index = scalesize;
        while(start < end){
            int count = 0;
            while (index < dataList.size() && dataList.get(index) < start + shift && dataList.get(index) < end){
                index++;
                count++;
            }

            writeOneBox(start, Math.min(start + shift, end), count, count / (float)dataList.size());
            start += shift;
        }
        writeOneBox(end, max, scalesize, scalesize / (float)dataList.size());
    }

    public void output(String filename) throws IOException {
        outputWriter = new FileWriter(filename);
        List<Long> statistics;

        outputWriter.write("TsFile文件结构分析\n");
        writeMainTitle("文件概述");
        writeSubTitle("文件路径");
        writeContent(tsFilePath + "。");
        writeSubTitle("文件大小");
        writeContent(fileSize + "字节（" + (fileSize/Math.pow(1024, 3)) + "GB）。");

        writeMainTitle("FileMetaData");
        writeContent("FileMetaData全文件只有一个，" +
                "大小为" + fileMetadataSize + "字节，" +
                "指向了" + rowGroupBlockMetaDataSizeList.size() + "个RowGroupBlockMetaData。");

        writeMainTitle("RowGroupsOfDeltaObjectMetaData");
        writeContent("RowGroupsOfDeltaObjectMetaData全文件有" + rowGroupBlockMetaDataSizeList.size() + "个。");
        writeSubTitle("大小");
        statistics = getSimpleStatistics(rowGroupBlockMetaDataSizeList);
        writeContent("RowGroupsOfDeltaObjectMetaData的平均大小为" + (statistics.get(2) / (float)rowGroupBlockMetaDataSizeList.size()) + "字节，" +
                "最小为" + statistics.get(0) + "字节，最大为" + statistics.get(1) + "字节。" +
                "其中具体分布统计如下（单位为字节）。");
        writeDistribution(rowGroupBlockMetaDataSizeList);
        writeSubTitle("内部结构");
        statistics = getSimpleStatistics(rowGroupBlockMetaDataContentList);
        writeContent("每个RowGroupsOfDeltaObjectMetaData平均指向" + (statistics.get(2) / (float)rowGroupBlockMetaDataContentList.size()) + "个RowGroup，" +
                "最少指向" + statistics.get(0) + "个，最多" + statistics.get(1) + "个。" +
                "其中具体分布统计如下（单位为个）。");
        writeDistribution(rowGroupBlockMetaDataContentList);

        writeMainTitle("RowGroup");
        writeContent("RowGroup全文件有" + rowGroupMetaDataSizeList.size() + "个。");
        writeSubTitle("大小");
        statistics = getSimpleStatistics(rowGroupMetaDataSizeList);
        writeContent("RowGroup的平均大小为" + (statistics.get(2) / (float)rowGroupMetaDataSizeList.size()) + "字节，" +
                "最小为" + statistics.get(0) + "字节，最大为" + statistics.get(1) + "字节。" +
                "其中具体分布统计如下（单位为字节）。");
        writeDistribution(rowGroupMetaDataSizeList);
        writeSubTitle("内部结构");
        statistics = getSimpleStatistics(rowGroupMetaDataContentList);
        writeContent("每个RowGroup平均包含" + (statistics.get(2) / (float)rowGroupMetaDataContentList.size()) + "个TimeSeriesChunk，" +
                "最少包含" + statistics.get(0) + "个，最多" + statistics.get(1) + "个。" +
                "其中具体分布统计如下（单位为个）。");
        writeDistribution(rowGroupMetaDataContentList);

        writeMainTitle("TimeSeriesChunk");
        writeContent("TimeSeriesChunk全文件有" + timeSeriesChunkMetaDataSizeList.size() + "个。");
        writeSubTitle("大小");
        statistics = getSimpleStatistics(timeSeriesChunkMetaDataSizeList);
        writeContent("TimeSeriesChunk的平均大小为" + (statistics.get(2) / (float)timeSeriesChunkMetaDataSizeList.size()) + "字节，" +
                "最小为" + statistics.get(0) + "字节，最大为" + statistics.get(1) + "字节。" +
                "其中具体分布统计如下（单位为字节）。");
        writeDistribution(timeSeriesChunkMetaDataSizeList);
        writeSubTitle("内部结构");
        statistics = getSimpleStatistics(timeSeriesChunkMetaDataContentList);
        writeContent("每个TimeSeriesChunk平均包含" + (statistics.get(2) / (float)timeSeriesChunkMetaDataContentList.size()) + "个Page，" +
                "最少包含" + statistics.get(0) + "个，最多" + statistics.get(1) + "个。" +
                "其中具体分布统计如下（单位为个）。");
        writeDistribution(timeSeriesChunkMetaDataContentList);

        writeMainTitle("Page");
        writeContent("Page全文件有" + pageSizeList.size() + "个。");
        writeSubTitle("大小");
        statistics = getSimpleStatistics(pageSizeList);
        writeContent("Page的平均大小为" + (statistics.get(2) / (float)pageSizeList.size()) + "字节，" +
                "最小为" + statistics.get(0) + "字节，最大为" + statistics.get(1) + "字节。" +
                "其中具体分布统计如下（单位为字节）。");
        writeDistribution(pageSizeList);
        writeSubTitle("内部结构");
        statistics = getSimpleStatistics(pageContentList);
        writeContent("每个Page平均存储" + (statistics.get(2) / (float)pageContentList.size()) + "个数据点，" +
                "最少存储" + statistics.get(0) + "个，最多" + statistics.get(1) + "个。" +
                "其中具体分布统计如下（单位为个）。");
        writeDistribution(pageContentList);

        outputWriter.close();
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
        String path = "/Users/East/Desktop/tsfile解析/data/1524261157000-1524291295414";
        TsFileAnalyzer analyzer = new TsFileAnalyzer(path);
        analyzer.analyze();
        analyzer.output("/Users/East/Desktop/1.txt");
    }
}
