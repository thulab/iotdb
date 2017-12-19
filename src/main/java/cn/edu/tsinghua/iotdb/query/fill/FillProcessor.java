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
import cn.edu.tsinghua.tsfile.timeseries.filter.utils.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.visitorImpl.DigestVisitor;
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
     * @return
     * @throws IOException
     */
    public static boolean getPreviousFillResult(DynamicOneColumnData result, ValueReader valueReader,
                                                long beforeTime, long queryTime)
            throws IOException {

//        if (beforeTime > valueReader.getEndTime()) {
//            return false;
//        }

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

            if (beforeTime > pageMaxTime) {
                pageReader.skipCurrentPage();
                offset += lastAvailable - bis.available();
            }

            InputStream page = pageReader.getNextPage();
            offset += lastAvailable - bis.available();
            valueReader.setDecoder(Decoder.getDecoderByType(pageHeader.getData_page_header().getEncoding(), dataType));
            long[] timestamps = valueReader.initTimeValue(page, pageHeader.data_page_header.num_rows, false);
            int timeIdx = 0;

            switch (dataType) {
                case INT32:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        int v = valueReader.decoder.readInt(page);

                        if (time < beforeTime) {
                            continue;
                        }
                        if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);
                            result.setInt(0, v);
                            continue;
                        }
                        if (time > queryTime) {
                            return true;
                        }
                    }
                    break;
                case INT64:
                    while (valueReader.decoder.hasNext(page)) {
                        long time = timestamps[timeIdx];
                        timeIdx++;

                        long v = valueReader.decoder.readLong(page);

                        if (time < beforeTime) {
                            continue;
                        }
                        if (time >= beforeTime && time <= queryTime) {
                            result.setTime(0, time);
                            result.setLong(0, v);
                            continue;
                        }
                        if (time > queryTime) {
                            return true;
                        }
                    }
                    break;
            }

        }



        return false;
    }
}
