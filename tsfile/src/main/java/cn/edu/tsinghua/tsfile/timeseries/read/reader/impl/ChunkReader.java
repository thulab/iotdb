package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public abstract class ChunkReader implements Reader {

    ChunkHeader chunkHeader;
    private ByteBuffer chunkDataBuffer;

    private boolean pageReaderInitialized;
    private PageReader pageReader;

    boolean hasCachedTimeValuePair;
    TimeValuePair cachedTimeValuePair;

    private UnCompressor unCompressor;
    private Decoder valueDecoder;
    private Decoder timeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder)
            , TSDataType.INT64);

    private DynamicOneColumnData data = null;
    private Filter filter = null;

    private long maxTombstoneTime;

    public ChunkReader(Chunk chunk) {
        this(chunk, null);
    }

    public ChunkReader(Chunk chunk, Filter filter) {
        this.filter = filter;
        this.chunkDataBuffer = chunk.getData();
        this.pageReaderInitialized = false;
        chunkHeader = chunk.getHeader();
        this.unCompressor = UnCompressor.getUnCompressor(chunkHeader.getCompressionType());
        valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCachedTimeValuePair) {
            return true;
        }

        //Judge whether next satisfied page exists
        while (true) {
            if (!pageReaderInitialized) {

                // construct next satisfied page header
                boolean hasMoreSatisfiedPage = constructPageReaderIfNextSatisfiedPageExists();

                // if there does not exist a satisfied page, return false
                if (!hasMoreSatisfiedPage) {
                    return false;
                }
                pageReaderInitialized = true;
            }

            // check whether there exists a satisfied time value pair in current page
            while (pageReader.hasNext()) {

                // read next time value pair
                TimeValuePair timeValuePair = pageReader.next();

                // check if next time value pair satisfy the condition
                if (timeValuePairSatisfied(timeValuePair) && timeValuePair.getTimestamp() > maxTombstoneTime) {

                    // cache next satisfied time value pair
                    this.cachedTimeValuePair = timeValuePair;
                    this.hasCachedTimeValuePair = true;

                    return true;
                }
            }
            pageReaderInitialized = false;
        }
    }

    @Override
    public boolean hasNextBatch() throws IOException {

        // construct next satisfied page header
        while (chunkDataBuffer.remaining() > 0) {
            // deserialize a PageHeader from chunkDataBuffer
            PageHeader pageHeader = getNextPageHeader();

            // if the current page satisfies
            if (pageSatisfied(pageHeader)) {
                pageReader = constructPageReaderForNextPage(pageHeader.getCompressedSize());
                if (pageReader.hasNextBatch()) {
                    data = pageReader.nextBatch();
                    return true;
                }
            } else {
                skipBytesInStreamByLength(pageHeader.getCompressedSize());
            }
        }
        return false;
    }


    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        }
        throw new IOException("No more timeValuePair in current Chunk");
    }

    @Override
    public DynamicOneColumnData nextBatch() {
        return data;
    }

    /**
     * Read page one by one from ByteBuffer and check the page header whether this page satisfies the filter.
     * Skip the unsatisfied pages and construct PageReader for the first page satisfied.
     *
     * @return whether there exists a satisfied page
     * @throws IOException exception when reading page
     */
    private boolean constructPageReaderIfNextSatisfiedPageExists() throws IOException {

        boolean gotNextPageReader = false;

        while (chunkDataBuffer.remaining() > 0 && !gotNextPageReader) {
            // deserialize a PageHeader from chunkDataBuffer
            PageHeader pageHeader = getNextPageHeader();

            // if the current page satisfies the filter
            if (pageSatisfied(pageHeader)) {
                pageReader = constructPageReaderForNextPage(pageHeader.getCompressedSize());
                gotNextPageReader = true;
            } else {
                skipBytesInStreamByLength(pageHeader.getCompressedSize());
            }
        }
        return gotNextPageReader;
    }


    private void skipBytesInStreamByLength(long length) {
        chunkDataBuffer.position(chunkDataBuffer.position() + (int) length);
    }

    public abstract boolean pageSatisfied(PageHeader pageHeader);

    public abstract boolean timeValuePairSatisfied(TimeValuePair timeValuePair);


    private PageReader constructPageReaderForNextPage(int compressedPageBodyLength)
            throws IOException {
        byte[] compressedPageBody = new byte[compressedPageBodyLength];

        // already in memory
        if (compressedPageBodyLength > chunkDataBuffer.remaining())
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + Arrays.toString(compressedPageBody) + ". Actual:" + chunkDataBuffer.remaining());

        chunkDataBuffer.get(compressedPageBody, 0, compressedPageBodyLength);
        valueDecoder.reset();
        return new PageReader(ByteBuffer.wrap(unCompressor.uncompress(compressedPageBody)),
                chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    }

    private PageHeader getNextPageHeader() throws IOException {
        return PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
    }

    @Override
    public void skipCurrentTimeValuePair() {
    }

    @Override
    public void close() {
    }

    public void setMaxTombstoneTime(long maxTombStoneTime) {
        this.maxTombstoneTime = maxTombStoneTime;
    }

    public long getMaxTombstoneTime() {
        return this.maxTombstoneTime;
    }


}
