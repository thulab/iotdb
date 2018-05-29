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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author East
 */
public class TsFileAnalyzer {

    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final double SCALE = 0.05;
    private static final int BOX_NUM = 20;
    private static final String DATA_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private String tsFilePath;
    private ITsRandomAccessFileReader randomAccessFileReader;

    private long fileSize;
    private long dataSize;
    private long metadataSize;
    private int filePathNum;
    private int fileRowNum;
    private long file_timestamp_min;
    private long file_timestamp_max;

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

        dataSize = 0;
        metadataSize = 0;
        filePathNum = 0;
        fileRowNum = 0;
        file_timestamp_min = -1;
        file_timestamp_max = -1;
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
        this.randomAccessFileReader = new TsRandomAccessLocalFileReader(tsFilePath);

        fileSize = randomAccessFileReader.length();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH);
        fileMetadataSize = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(fileSize - MAGIC_LENGTH - FOOTER_LENGTH - fileMetadataSize);
        byte[] buf = new byte[fileMetadataSize];
        randomAccessFileReader.read(buf, 0, buf.length);

        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        fileMetaData = new TsFileMetaDataConverter().toTsFileMetadata(ReadWriteThriftFormatUtils.readFileMetaData(metadataInputStream));

        int rgbCount = 0;
        int totalCount = fileMetaData.getDeltaObjectMap().size();
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
                Set<String> measurementIdSet = new HashSet<>();
                fileRowNum += rowGroupMetaData.getNumOfRows();

                for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
                    if(file_timestamp_min < 0 && file_timestamp_max < 0){
                        file_timestamp_min = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime();
                        file_timestamp_max = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime();
                    }else{
                        if(file_timestamp_min > timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime())
                            file_timestamp_min = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getStartTime();
                        if(file_timestamp_max < timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime())
                            file_timestamp_max = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData().getEndTime();
                    }

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
                    measurementIdSet.add(timeSeriesChunkMetaData.getProperties().getMeasurementUID());
                    timeSeriesChunkMetaDataRowNumList.add((int)timeSeriesChunkMetaData.getNumRows());
                }

                filePathNum += measurementIdSet.size();
            }

            rgbCount++;
            System.out.println(rgbCount / (float)totalCount);
        }

        for(int size : pageSizeList)
            dataSize += size;
        metadataSize = fileSize - dataSize;

        randomAccessFileReader.close();
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
        writeContent(start + "~" + end + "(" + (end - start + 1) + "):" + num + "," + rate * 100 + "%", "box");
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
        writeContent(statistics.get(2) / (float)dataList.size(), "average");
        writeContent(statistics.get(0),"min");
        writeContent(statistics.get(1),"max");
        writeEndTag(name + "_metadata_" + cate);
    }

    private List<Integer> getCountList(List<Integer> dataList, List<Integer> boxList){
        List<Integer> counts = new ArrayList<>();

        int index = 0;
        int count = 0;
        for(int box : boxList){
            while(index < dataList.size() && box > dataList.get(index)){
                index++;
                count++;
            }

            counts.add(count);
            count = 0;
        }

        return counts;
    }

    private List<Integer> getBoxList(List<Integer> dataList){
        List<Integer> boxList = new ArrayList<>();

        int scalesize = (int) (dataList.size() * SCALE);
        int max = dataList.get(dataList.size() - 1);
        int start = dataList.get(0);
        int end = dataList.get(dataList.size() - 1 - scalesize);
        int shift = (end - start) / BOX_NUM;
        if(shift <= 0)shift = 1;

        if(max - start + 1 <= BOX_NUM){
            for(int i = start + 1;i <= max + 1;i++){
                boxList.add(i);
            }
        }else if(end - start + 1 <= BOX_NUM){
            for(int i = start + 1;i <= end;i++){
                boxList.add(i);
            }
            if(max > end)boxList.add(max);

            int temp = boxList.remove(boxList.size() - 1);
            boxList.add(temp + 1);
        }else{
            for(int i = 1;i < BOX_NUM;i++){
                boxList.add(start + shift * i);
            }
            boxList.add(end);
            if(end < max)boxList.add(max);

            int temp = boxList.remove(boxList.size() - 1);
            boxList.add(temp + 1);
        }

        return boxList;
    }

    private void writeDistribution(List<Integer> dataList, String name, String cate) throws IOException {
        writeBeginTag(name + "_metadata_" + cate);

        Collections.sort(dataList);
        List<Integer> boxList = getBoxList(dataList);
        List<Integer> countList = getCountList(dataList, boxList);

        int start = dataList.get(0);
        for(int i = 0;i < Math.min(countList.size(), boxList.size());i++){
            writeOneBox(start, boxList.get(i) - 1, countList.get(i), countList.get(i) / (float)dataList.size());
            start = boxList.get(i);
        }

        writeEndTag(name + "_metadata_" + cate);
    }

    public void output(String filename) throws IOException {
        outputWriter = new FileWriter(filename);

        // file
        writeBeginTag("File");
        writeContent(tsFilePath, "file_path");
        writeContent(fileSize, "file_size");
        writeContent(fileSize/Math.pow(1024, 3), "file_size_GB");
        writeContent(dataSize, "data_size");
        writeContent(metadataSize, "metadata_size");
        writeContent(dataSize / (double)fileSize, "data_rate");
        writeContent(rowGroupBlockMetaDataSizeList.size(), "file_deltaobject_num");
        writeContent(fileMetaData.getTimeSeriesList().size(), "file_timeseries_num");
        writeContent(filePathNum, "file_path_num");
        writeContent(fileRowNum, "file_points_num");
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATA_FORMAT);
        writeContent(dateFormat.format(new Date(file_timestamp_min)), "file_timestamp_min");
        writeContent(dateFormat.format(new Date(file_timestamp_max)), "file_timestamp_max");
        writeEndTag("File");

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
        args = new String[]{"/Users/East/Desktop/tsfile解析/data/1524261157000-1524291295414", "/Users/East/Desktop/tsfile解析/data/report.txt"};
        if (args == null || args.length < 2) {
            System.out.println("[ERROR] Too few params input, please input path for both tsfile and output report.");
            return;
        }

        String path = args[0];
        TsFileAnalyzer analyzer = new TsFileAnalyzer(path);
        analyzer.analyze();
        analyzer.output(args[1]);
    }
}
