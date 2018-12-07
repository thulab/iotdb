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
import cn.edu.tsinghua.tsfile.timeseries.read.reader.BatchData;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.Reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public abstract class ChunkReader implements Reader {

    ChunkHeader chunkHeader;
    private ByteBuffer chunkDataBuffer;

    private PageReader pageReader;

    private UnCompressor unCompressor;
    private Decoder valueDecoder;
    private Decoder timeDecoder = Decoder.getDecoderByType(TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().timeSeriesEncoder)
            , TSDataType.INT64);

    private Filter filter = null;

    private BatchData data = null;
    private boolean hasCachedData;

    private long maxTombstoneTime;


    public ChunkReader(Chunk chunk) {
        this(chunk, null);
    }

    public ChunkReader(Chunk chunk, Filter filter) {
        this.filter = filter;
        this.hasCachedData = false;
        this.chunkDataBuffer = chunk.getData();
        chunkHeader = chunk.getHeader();
        this.unCompressor = UnCompressor.getUnCompressor(chunkHeader.getCompressionType());
        valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    }


    @Override
    public boolean hasNextBatch() throws IOException {

        if (hasCachedData)
            return true;

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
    public BatchData nextBatch() {
        hasCachedData = false;
        return data;
    }

    @Override
    public BatchData currentBatch() {
        return data;
    }

    private void skipBytesInStreamByLength(long length) {
        chunkDataBuffer.position(chunkDataBuffer.position() + (int) length);
    }

    public abstract boolean pageSatisfied(PageHeader pageHeader);


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
    public void close() {
    }

    public void setMaxTombstoneTime(long maxTombStoneTime) {
        this.maxTombstoneTime = maxTombStoneTime;
    }

    public long getMaxTombstoneTime() {
        return this.maxTombstoneTime;
    }


}
