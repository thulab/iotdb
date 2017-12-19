package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.query.dataset.InsertDynamicData;
import cn.edu.tsinghua.iotdb.query.reader.ReaderUtils;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.IntervalTimeVisitor;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.SingleValueVisitor;
import cn.edu.tsinghua.tsfile.timeseries.read.PageReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class contains the Fill process method.
 */
public class FillProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(FillProcessor.class);

    /**
     * Return false if we haven't get the correct previous value before queryTime.
     *
     * @param result
     * @param valueReader
     * @param beforeTime
     * @param queryTime
     * @param timeFilter
     * @param updateTrue
     * @param updateFalse Note that, updateFalse is got from value filter, in fill function, there is only time,
     *                    so updateFalse is useless
     * @return
     * @throws IOException
     */
    public static boolean getPreviousFillResultInFile(DynamicOneColumnData result, ValueReader valueReader,
                                                      long beforeTime, long queryTime, SingleSeriesFilterExpression timeFilter,
                                                      DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse)
            throws IOException {

        if (beforeTime > valueReader.getEndTime()) {
            return false;
        }

        IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !intervalTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            return false;
        }

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        long offset = valueReader.getFileOffset();
        while ((offset - valueReader.getFileOffset()) <= valueReader.totalSize) {
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(offset);
            long lastAvailable = bis.available();

            PageReader pageReader = new PageReader(bis, compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            // TODO this branch need to be covered by test case
            if (beforeTime > pageMaxTime) {
                pageReader.skipCurrentPage();
                offset += lastAvailable - bis.available();
                continue;
            }

            InputStream page = pageReader.getNextPage();
            offset += lastAvailable - bis.available();
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
            long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            int timeIdx = 0;

            SingleValueVisitor singleValueVisitor = null;
            if (timeFilter != null) {
                singleValueVisitor = new SingleValueVisitor(timeFilter);
            }
            switch (dataType) {
                case INT32:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        int v = valueReader.decoder.readInt(page);

                        // TODO this branch need to be covered by test case for overflow delete operation
                        if (timeFilter != null && !singleValueVisitor.verify(time)) {
                            continue;
                        }

                        if (time < beforeTime) {
                            continue;
                        } else if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);

                            // TODO this branch need to be covered by test case
                            while (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2 + 1) < time) {
                                updateTrue.curIdx ++;
                            }
                            if (updateTrue.curIdx < updateTrue.valueLength && updateTrue.getTime(updateTrue.curIdx*2) <= time
                                    && updateTrue.getTime(updateTrue.curIdx*2) >= time) {
                                v = updateTrue.getInt(updateTrue.curIdx);
                            }
                            result.setInt(0, v);
                            continue;
                        } else {
                            return true;
                        }
                    }
                    break;
                case INT64:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        // TODO this branch need to be covered by test case for overflow delete operation
                        long v = valueReader.decoder.readLong(page);

                        if (timeFilter != null && !singleValueVisitor.verify(time)) {
                            continue;
                        }

                        if (time < beforeTime) {
                            continue;
                        }else if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);
                            result.setLong(0, v);
                            continue;
                        } else {
                            return true;
                        }
                    }
                    break;
            }

        }

        return false;
    }

    public static void getPreviousFillResultInMemory(DynamicOneColumnData result, InsertDynamicData insertMemoryData,
                                                        long beforeTime, long queryTime)
            throws IOException {

        while (insertMemoryData.hasInsertData()) {
            long time = insertMemoryData.getCurrentMinTime();
            if (time > queryTime) {
                break;
            }
            if (time >= beforeTime && time <= queryTime) {
                switch (result.dataType) {
                    case INT32:
                        if (result.timeLength == 0) {
                            result.putTime(time);
                            result.putInt(insertMemoryData.getCurrentIntValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime < time) {
                                result.setTime(0, time);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            }
                        }
                        break;
                    case INT64:
                        if (result.timeLength == 0) {
                            result.putTime(time);
                            result.putLong(insertMemoryData.getCurrentIntValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime < time) {
                                result.setTime(0, time);
                                result.setLong(0, insertMemoryData.getCurrentIntValue());
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
            insertMemoryData.removeCurrentValue();
        }
    }


    public static boolean getLinearFillResultInFile(DynamicOneColumnData result, ValueReader valueReader,
                                                    long beforeTime, long queryTime, long afterTime, SingleSeriesFilterExpression timeFilter,
                                                    DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse)
            throws IOException {

        if (beforeTime > valueReader.getEndTime()) {
            return false;
        }
        if (afterTime < valueReader.getStartTime()) {
            return true;
        }
        IntervalTimeVisitor intervalTimeVisitor = new IntervalTimeVisitor();
        if (timeFilter != null && !intervalTimeVisitor.satisfy(timeFilter, valueReader.getStartTime(), valueReader.getEndTime())) {
            return false;
        }

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        long offset = valueReader.getFileOffset();
        while ((offset - valueReader.getFileOffset()) <= valueReader.totalSize) {
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(offset);
            long lastAvailable = bis.available();

            PageReader pageReader = new PageReader(bis, compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();

            long pageMinTime = pageHeader.data_page_header.min_timestamp;
            long pageMaxTime = pageHeader.data_page_header.max_timestamp;

            // TODO test covered
            if (beforeTime > pageMaxTime) {
                pageReader.skipCurrentPage();
                offset += lastAvailable - bis.available();
                continue;
            }
            if (afterTime < pageMinTime) {
                return true;
            }
            InputStream page = pageReader.getNextPage();
            offset += lastAvailable - bis.available();
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
            long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            int timeIdx = 0;

            SingleValueVisitor singleValueVisitor = null;
            if (timeFilter != null) {
                singleValueVisitor = new SingleValueVisitor(timeFilter);
            }
            switch (dataType) {
                case INT32:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        int v = valueReader.decoder.readInt(page);

                        // TODO this branch need to be covered by test case for overflow delete operation
                        if (timeFilter != null && !singleValueVisitor.verify(time)) {
                            continue;
                        }

                        if (time < beforeTime) {
                            continue;
                        } else if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);
                            result.setInt(0, v);
                            continue;
                        } else {
                            result.setTime(1, time);
                            result.setInt(1, v);
                            return true;
                        }
                    }
                    break;
                case INT64:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        long v = valueReader.decoder.readLong(page);

                        // TODO this branch need to be covered by test case for overflow delete operation
                        if (timeFilter != null && !singleValueVisitor.verify(time)) {
                            continue;
                        }

                        if (time < beforeTime) {
                            continue;
                        }else if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);
                            result.setLong(0, v);
                            continue;
                        } else {
                            result.setTime(1, time);
                            result.setLong(1, v);
                            return true;
                        }
                    }
                    break;
            }

        }

        return false;
    }

    public static void getLinearFillResultInMemory(DynamicOneColumnData result, InsertDynamicData insertMemoryData,
                                                        long beforeTime, long queryTime, long afterTime)
            throws IOException {

        while (insertMemoryData.hasInsertData()) {
            long time = insertMemoryData.getCurrentMinTime();

            if (time >= beforeTime && time <= queryTime) {
                switch (result.dataType) {
                    case INT32:
                        if (result.timeLength == 0) {
                            result.putTime(time);
                            result.putInt(insertMemoryData.getCurrentIntValue());
                        } else {
                            long existTime = result.getTime(0);
                            // TODO existTime == time is a special situation
                            if (existTime <= time) {
                                result.setTime(0, time);
                                result.setInt(0, insertMemoryData.getCurrentIntValue());
                            }
                        }
                        break;
                    case INT64:
                        if (result.timeLength == 0) {
                            result.putTime(time);
                            result.putLong(insertMemoryData.getCurrentIntValue());
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= time) {
                                result.setTime(0, time);
                                result.setLong(0, insertMemoryData.getCurrentIntValue());
                            }
                        }
                        break;
                    default:
                        break;
                }
            } else if (time > queryTime) {
                switch (result.dataType) {
                    case INT32:
                        if (result.timeLength == 0) {
                            return;
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= queryTime) {
                                result.putTime(time);
                                result.putInt(insertMemoryData.getCurrentIntValue());
                                return;
                            } else {
                                return;
                            }
                        }
                    case INT64:
                        if (result.timeLength == 0) {
                            return;
                        } else {
                            long existTime = result.getTime(0);
                            if (existTime <= queryTime) {
                                result.putTime(time);
                                result.putLong(insertMemoryData.getCurrentLongValue());
                                return;
                            } else {
                                return;
                            }
                        }
                    default:
                        break;
                }
                break;
            }
            insertMemoryData.removeCurrentValue();
        }
    }
}
