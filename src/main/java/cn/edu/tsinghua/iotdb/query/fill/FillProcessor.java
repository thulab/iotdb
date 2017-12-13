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

    public static boolean getFillResult(DynamicOneColumnData result, ValueReader valueReader,
                               DynamicOneColumnData updateTrue, DynamicOneColumnData updateFalse, InsertDynamicData insertMemoryData,
                               long beforeTime, long queryTime)
            throws IOException {

        TSDataType dataType = valueReader.getDataType();
        CompressionTypeName compressionTypeName = valueReader.compressionTypeName;

        DigestVisitor digestVisitor = new DigestVisitor();

        long offset = valueReader.getFileOffset();
        while ((offset - valueReader.getFileOffset()) <= valueReader.totalSize) {
            ByteArrayInputStream bis = valueReader.initBAISForOnePage(offset);
            long lastAvailable = bis.available();
            PageReader pageReader = new PageReader(bis, compressionTypeName);
            PageHeader pageHeader = pageReader.getNextPageHeader();

        }


        return false;
    }
}
