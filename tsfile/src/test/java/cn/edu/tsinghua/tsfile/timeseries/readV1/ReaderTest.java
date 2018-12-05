package cn.edu.tsinghua.tsfile.timeseries.readV1;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.impl.SeriesReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


public class ReaderTest {

    private static final String FILE_PATH = TsFileGeneratorForTest.outputDataFile;
    private TsFileSequenceReader fileReader;
    private MetadataQuerierByFileImpl metadataQuerierByFile;
    private int rowCount = 1000000;

    @Before
    public void before() throws InterruptedException, WriteProcessException, IOException {
        TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder = "TS_2DIFF";
        TsFileGeneratorForTest.generateFile(rowCount, 10 * 1024 * 1024, 10000);
        fileReader = new TsFileSequenceReader(FILE_PATH);
        metadataQuerierByFile = new MetadataQuerierByFileImpl(fileReader);
    }

    @After
    public void after() throws IOException {
        fileReader.close();
        TsFileGeneratorForTest.after();
    }

    @Test
    public void readTest() throws IOException {
        int count = 0;
        ChunkLoaderImpl seriesChunkLoader = new ChunkLoaderImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s1"));

        Reader seriesReader = new SeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);
        long startTime = TsFileGeneratorForTest.START_TIMESTAMP;
        long startTimestamp = System.currentTimeMillis();
        BatchData data = null;

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                Assert.assertEquals(startTime, data.getTime());
                data.next();
                startTime++;
                count++;
            }
        }
        long endTimestamp = System.currentTimeMillis();
        Assert.assertEquals(rowCount, count);
        System.out.println("SeriesReadTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);

        chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s4"));
        seriesReader = new SeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);
        count = 0;
        startTimestamp = System.currentTimeMillis();

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                data.next();
                startTime ++;
                count++;
            }
        }
        endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }

    @Test
    public void readWithFilterTest() throws IOException {
        ChunkLoaderImpl seriesChunkLoader = new ChunkLoaderImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s1"));

        Filter filter = new FilterFactory().or(
                FilterFactory.and(TimeFilter.gt(1480563570029L), TimeFilter.lt(1480563570033L)),
                FilterFactory.and(ValueFilter.gtEq(9520331), ValueFilter.ltEq(9520361)));
        SeriesFilter seriesFilter = new SeriesFilter(new Path("d1.s1"), filter);
        Reader seriesReader = new SeriesReaderWithFilter(seriesChunkLoader, chunkMetaDataList, seriesFilter.getFilter());

        long startTimestamp = System.currentTimeMillis();
        int count = 0;

        BatchData data;

        long aimedTimestamp = 1480563570030L;

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                Assert.assertEquals(aimedTimestamp++, data.getTime());
                data.next();
                count++;
            }
        }


        long endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadWithFilterTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }
}
