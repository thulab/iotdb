package cn.edu.tsinghua.tsfile.timeseries.readV1;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.ChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
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

        List<TimeValuePair> timeValuePairList = new ArrayList<>();
        int count = 0;
        while (seriesReader.hasNext()) {
            TimeValuePair timeValuePair = seriesReader.next();
            if (count % 100 == 0) {
                timeValuePairList.add(new TimeValuePair(timeValuePair.getTimestamp() - 1, null));
                timeValuePairList.add(timeValuePair);
            }
            count++;
        }

        long startTimestamp = System.currentTimeMillis();
        count = 0;

        SeriesReaderByTimestamp seriesReaderFromSingleFileByTimestamp = new SeriesReaderByTimestamp(seriesChunkLoader, chunkMetaDataList);

        for (TimeValuePair timeValuePair : timeValuePairList) {
            TsPrimitiveType value = seriesReaderFromSingleFileByTimestamp.getValueInTimestamp(timeValuePair.getTimestamp());
            Assert.assertEquals(timeValuePair.getValue(), value);
            count ++;
        }
        long endTimestamp = System.currentTimeMillis();
        System.out.println("SeriesReadWithFilterTest. [Time used]: " + (endTimestamp - startTimestamp) +
                " ms. [Read Count]: " + count);
    }
}
