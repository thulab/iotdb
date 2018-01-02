package cn.edu.tsinghua.iotdb.query;

import cn.edu.tsinghua.iotdb.query.dataset.InsertData;
import cn.edu.tsinghua.iotdb.query.engine.AggregateEngine;
import cn.edu.tsinghua.iotdb.service.IoTDB;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;
import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.TSEncodingConverter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.SeriesWriterImpl;
import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * This class is a test for <code>InsertData</code>
 */
public class InsertDataTest {

    private String deltaObjectId = "device";
    private String measurementId = "sensor";
    private MeasurementDescriptor descriptor = new MeasurementDescriptor(measurementId, TSDataType.FLOAT, TSEncoding.RLE);


    @Test
    public void basicTest() throws IOException {
        TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
        config.duplicateIncompletedPage = true;
        SeriesWriterImpl writer = new SeriesWriterImpl(deltaObjectId, descriptor, new PageWriterImpl(descriptor), 20);
        for (long i = 100;i <= 500; i++) {
            writer.write(i, (float)i % 50);
        }
        for (long i = 700;i <= 1000; i++) {
            writer.write(i, (float)i);
        }

        List<Object> writeList = writer.query();
        Pair<List<ByteArrayInputStream>, CompressionTypeName> pair = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) writeList.get(1);
        List<ByteArrayInputStream> sealedPageList = pair.left;
        CompressionTypeName compressionTypeName = pair.right;
        DynamicOneColumnData lastPageData = (DynamicOneColumnData) writeList.get(0);
        DynamicOneColumnData overflowInsert = buildOverflowInsertData();
        DynamicOneColumnData overflowUpdateTrue = buildOverflowUpdateTrue();
        DynamicOneColumnData overflowUpdateFalse = buildOverflowUpdateFalse();

        InsertData insertData = new InsertData(TSDataType.FLOAT, compressionTypeName, sealedPageList, lastPageData,
                overflowInsert, overflowUpdateTrue, overflowUpdateFalse, null, null);

        while (insertData.hasInsertData()) {
            long time = insertData.getCurrentMinTime();
            float value = insertData.getCurrentFloatValue();
            System.out.println(time + "," + value);
            insertData.removeCurrentValue(); 
        }
    }

    private DynamicOneColumnData buildOverflowInsertData() {
        DynamicOneColumnData overflowInsert = new DynamicOneColumnData(TSDataType.FLOAT, true);
        for (int i = 200;i <= 220;i++) {
            overflowInsert.putTime(i);
            overflowInsert.putFloat(i);
        }
        return overflowInsert;
    }

    private DynamicOneColumnData buildOverflowUpdateTrue() {
        DynamicOneColumnData ans = new DynamicOneColumnData(TSDataType.FLOAT, true);
        ans.putTime(490);
        ans.putTime(500);
        ans.putFloat(7.0f);
        return ans;
    }

    private DynamicOneColumnData buildOverflowUpdateFalse() {
        DynamicOneColumnData ans = new DynamicOneColumnData(TSDataType.FLOAT, true);
        ans.putTime(990);
        ans.putTime(1000);
        ans.putFloat(-4.0f);
        return ans;
    }
}
