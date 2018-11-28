package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.encoding.decoder.Decoder;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Chunk;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public abstract class SeriesChunkReader implements SeriesReader {

    ChunkHeader chunkHeader;
    private ByteBuffer chunkDataBuffer;

    private boolean pageReaderInitialized;
    private PageDataReader pageDataReader;

    boolean hasCachedTimeValuePair;
    TimeValuePair cachedTimeValuePair;

    private UnCompressor unCompressor;
    private Decoder valueDecoder;
    private Decoder timeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder)
            , TSDataType.INT64);

    private long maxTombstoneTime;

    public SeriesChunkReader(Chunk chunk) {
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
            while (pageDataReader.hasNext()) {

                // read next time value pair
                TimeValuePair timeValuePair = pageDataReader.next();

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
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair;
        }
        throw new IOException("No more timeValuePair in current Chunk");
    }

    /**
     * Read page one by one from InputStream and check the page header whether this page satisfies the filter.
     * Skip the unsatisfied pages and construct PageDataReader for the first page satisfied.
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
                pageDataReader = constructPageReaderForNextPage(pageHeader.getCompressedSize());
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


    private PageDataReader constructPageReaderForNextPage(int compressedPageBodyLength)
            throws IOException {
        byte[] compressedPageBody = new byte[compressedPageBodyLength];

        // already in memory
        if (compressedPageBodyLength > chunkDataBuffer.remaining())
            throw new IOException("unexpected byte read length when read compressedPageBody. Expected:"
                    + Arrays.toString(compressedPageBody) + ". Actual:" + chunkDataBuffer.remaining());

        chunkDataBuffer.get(compressedPageBody, 0, compressedPageBodyLength);
        valueDecoder.reset();
        return new PageDataReader(ByteBuffer.wrap(unCompressor.uncompress(compressedPageBody)),
                chunkHeader.getDataType(), valueDecoder, timeDecoder);
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
