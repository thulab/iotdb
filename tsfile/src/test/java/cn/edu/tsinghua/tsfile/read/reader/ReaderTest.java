package cn.edu.tsinghua.tsfile.read.reader;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.read.filter.TimeFilter;
import cn.edu.tsinghua.tsfile.read.filter.ValueFilter;
import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.read.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.read.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.read.common.BatchData;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReader;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderWithFilter;
import cn.edu.tsinghua.tsfile.read.reader.series.SeriesReaderWithoutFilter;
import cn.edu.tsinghua.tsfile.exception.write.WriteProcessException;
import cn.edu.tsinghua.tsfile.utils.TsFileGeneratorForTest;
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

        SeriesReader seriesReader = new SeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);
        long startTime = TsFileGeneratorForTest.START_TIMESTAMP;
        BatchData data = null;

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                Assert.assertEquals(startTime, data.currentTime());
                data.next();
                startTime++;
                count++;
            }
        }
        Assert.assertEquals(rowCount, count);

        chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s4"));
        seriesReader = new SeriesReaderWithoutFilter(seriesChunkLoader, chunkMetaDataList);
        count = 0;

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                data.next();
                startTime ++;
                count++;
            }
        }
    }

    @Test
    public void readWithFilterTest() throws IOException {
        ChunkLoaderImpl seriesChunkLoader = new ChunkLoaderImpl(fileReader);
        List<ChunkMetaData> chunkMetaDataList = metadataQuerierByFile.getChunkMetaDataList(new Path("d1.s1"));

        Filter filter = new FilterFactory().or(
                FilterFactory.and(TimeFilter.gt(1480563570029L), TimeFilter.lt(1480563570033L)),
                FilterFactory.and(ValueFilter.gtEq(9520331), ValueFilter.ltEq(9520361)));
        SeriesFilter seriesFilter = new SeriesFilter(new Path("d1.s1"), filter);
        SeriesReader seriesReader = new SeriesReaderWithFilter(seriesChunkLoader, chunkMetaDataList, seriesFilter.getFilter());

        BatchData data;

        long aimedTimestamp = 1480563570030L;

        while(seriesReader.hasNextBatch()) {
            data = seriesReader.nextBatch();
            while (data.hasNext()) {
                Assert.assertEquals(aimedTimestamp++, data.currentTime());
                data.next();
            }
        }
    }
}
