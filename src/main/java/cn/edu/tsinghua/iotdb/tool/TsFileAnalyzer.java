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

/**
 * @author East
 */
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

    private FileWriter outputWriter;
    private int tageNum = 0;

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

    private void writeTag() throws IOException {
        for(int i = 0;i < tageNum;i++)
            outputWriter.write("\t");
    }

    private void writeBeginTag(String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">\n");
        tageNum++;
    }

    private void writeEndTag(String key) throws IOException {
        tageNum--;
        writeTag();
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(int content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write("" + content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(double content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write("" + content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeContent(String content, String key) throws IOException {
        writeTag();
        outputWriter.write("<" + key + ">");
        outputWriter.write(content);
        outputWriter.write("</" + key + ">\n");
    }

    private void writeOneBox(int start, int end, int num, float rate) throws IOException {
        writeTag();
        writeContent(start + "~" + Math.max(end - 1, start) + "(" + (end - start) + "):" + num + "," + rate * 100 + "%", "box");
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

    private void writeStatistics(List<Integer> dataList, String name, String cate) throws IOException {
        List<Long> statistics = getSimpleStatistics(dataList);
        writeBeginTag(name + "_metadata_" + cate);
        writeTag();
        writeContent(statistics.get(2) / (float)dataList.size(), "average");
        writeTag();
        writeContent(statistics.get(0),"min");
        writeTag();
        writeContent(statistics.get(1),"max");
        writeEndTag(name + "_metadata_" + cate);
    }

    private void writeDistribution(List<Integer> dataList, String name, String cate) throws IOException {
        writeBeginTag(name + "_metadata_" + cate);

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
                if(boxes.containsKey(i))writeOneBox(i, i + 1, boxes.get(i), boxes.get(i) / (float)dataList.size());
            }
        }
        else {
            writeOneBox(min, start, scalesize, scalesize / (float) dataList.size());
            int index = scalesize;
            while (start < end) {
                int count = 0;
                while (index < dataList.size() && dataList.get(index) < start + shift && dataList.get(index) < end) {
                    index++;
                    count++;
                }

                writeOneBox(start, Math.min(start + shift, end), count, count / (float) dataList.size());
                start += shift;
            }
            writeOneBox(end, max, scalesize, scalesize / (float) dataList.size());
        }

        writeEndTag(name + "_metadata_" + cate);
    }

    public void output(String filename) throws IOException {
        outputWriter = new FileWriter(filename);

        // file
        writeContent(tsFilePath, "file_path");
        writeContent(fileSize, "file_size");
        writeContent(fileSize/Math.pow(1024, 3), "file_size_GB");

        // file metadata
        writeBeginTag("FileMetaData");
        writeContent(1, "file_metadata_num");
        writeContent(fileMetadataSize, "file_metadata_size");
        writeContent(rowGroupBlockMetaDataSizeList.size(), "file_metadata_children_num");
        writeEndTag("FileMetaData");

        // row groups of deltaobject metadata
        writeBeginTag("RowGroupsOfDeltaObjectMetaData");
        writeContent(rowGroupBlockMetaDataSizeList.size(), "rowgroupsofdeltaobject_metadata_num");
        writeStatistics(rowGroupBlockMetaDataSizeList, "rowgroupsofdeltaobject", "size");
        writeDistribution(rowGroupBlockMetaDataSizeList, "rowgroupsofdeltaobject", "size_distribution");
        writeStatistics(rowGroupBlockMetaDataSizeList, "rowgroupsofdeltaobject", "children_num");
        writeDistribution(rowGroupBlockMetaDataContentList, "rowgroupsofdeltaobject", "children_num_distribution");
        writeEndTag("RowGroupsOfDeltaObjectMetaData");

        // row group metadata
        writeBeginTag("RowGroup");
        writeContent(rowGroupMetaDataSizeList.size(), "rowgroup_metadata_num");
        writeStatistics(rowGroupMetaDataSizeList, "rowgroup", "size");
        writeDistribution(rowGroupMetaDataSizeList, "rowgroup", "size_distribution");
        writeStatistics(rowGroupMetaDataContentList, "rowgroup", "children_num");
        writeDistribution(rowGroupMetaDataContentList, "rowgroup", "children_num_distribution");
        writeEndTag("RowGroup");

        // time series chunk metadata
        writeBeginTag("TimeSeriesChunk");
        writeContent(timeSeriesChunkMetaDataSizeList.size(), "timeserieschunk_metadata_num");
        writeStatistics(timeSeriesChunkMetaDataSizeList, "timeserieschunk", "size");
        writeDistribution(timeSeriesChunkMetaDataSizeList, "timeserieschunk", "size_distribution");
        writeStatistics(timeSeriesChunkMetaDataContentList, "timeserieschunk", "children_num");
        writeDistribution(timeSeriesChunkMetaDataContentList, "timeserieschunk", "children_num_distribution");
        writeEndTag("TimeSeriesChunk");

        // page
        writeBeginTag("Page");
        writeContent(pageSizeList.size(), "page_metadata_num");
        writeStatistics(pageSizeList, "page", "size");
        writeDistribution(pageSizeList, "page", "size_distribution");
        writeStatistics(pageContentList, "page", "children_num");
        writeDistribution(pageContentList, "page", "children_num_distribution");
        writeEndTag("Page");

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
