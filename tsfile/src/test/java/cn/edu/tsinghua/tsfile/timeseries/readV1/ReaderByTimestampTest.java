package cn.edu.tsinghua.tsfile.timeseries.readV1;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderByTimestamp;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ReaderByTimestampTest {

    private static final String FILE_PATH = TsFileGeneratorForSeriesReaderByTimestamp.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private int rowCount = 1000000;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForSeriesReaderByTimestamp.generateFile(rowCount, 10 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);//TODO remove this class
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);

    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForSeriesReaderByTimestamp.after();
    }

    @Test
    public void readByTimestamp() throws IOException {
        ChunkLoaderImpl seriesChunkLoader = new ChunkLoaderImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s1"));
        Reader seriesReader = new SeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);

        List<Long> timeList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        int count = 0;
        BatchData data = null;

        while (seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                timeList.add(data.currentTime() - 1);
                valueList.add(null);
                timeList.add(data.currentTime());
                valueList.add(data.currentValue());
                data.next();
                count++;
            }
        }

        long startTimestamp = System.currentTimeMillis();
        count = 0;

        SeriesReaderByTimestamp seriesReaderFromSingleFileByTimestamp = new SeriesReaderByTimestamp(seriesChunkLoader, chunkMetaDataList);

        for (long time : timeList) {
            Object value = seriesReaderFromSingleFileByTimestamp.getValueInTimestamp(time);
            if (value == null)
                Assert.assertNull(valueList.get(count));
            else
                Assert.assertEquals(valueList.get(count), value);
            count++;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadWithFilterTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }
}
