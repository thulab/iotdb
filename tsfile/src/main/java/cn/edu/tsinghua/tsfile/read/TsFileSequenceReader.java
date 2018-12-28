package cn.edu.tsinghua.tsfile.read;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.read.reader.DefaultTsFileInput;
import cn.edu.tsinghua.tsfile.read.reader.TsFileInput;
import cn.edu.tsinghua.tsfile.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.compress.UnCompressor;
import cn.edu.tsinghua.tsfile.file.footer.ChunkGroupFooter;
import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeviceMetadataIndex;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.read.common.Chunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class TsFileSequenceReader {
    private TsFileInput tsFileInput;
    private long fileMetadataPos;
    private int fileMetadataSize;
    private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);

    /**
     * create a file reader of the given file.
     * The reader will read the tail of the file to get the file metadata size.
     * Then the reader will skip the first TSFileConfig.MAGIC_STRING.length() bytes of the file
     * for preparing reading real data.
     *
     * @param file the data file
     * @throws IOException If some I/O error occurs
     */
    public TsFileSequenceReader(String file) throws IOException {
        this(file, true);
    }


    TsFileSequenceReader(String file, boolean loadMetadataSize) throws IOException {
        tsFileInput = new DefaultTsFileInput(Paths.get(file));
        if (loadMetadataSize)
            loadMetadataSize();
    }

    private void loadMetadataSize() throws IOException {
        ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
        tsFileInput.read(metadataSize, tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES);
        metadataSize.flip();
        //read file metadata size and position
        fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);
        fileMetadataPos = tsFileInput.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES - fileMetadataSize;
        //skip the magic header
        tsFileInput.position(TSFileConfig.MAGIC_STRING.length());
    }


    /**
     * @param input            the input of a tsfile. The current position should be a markder and then a chunk Header,
     *                         rather than the magic number
     * @param fileMetadataPos  the position of the file metadata in the TsFileInput
     *                         from the beginning of the input to the current position
     * @param fileMetadataSize the byte size of the file metadata in the input
     * @throws IOException If some I/O error occurs
     */
    public TsFileSequenceReader(TsFileInput input, long fileMetadataPos, int fileMetadataSize) throws IOException {
        this.tsFileInput = input;
        this.fileMetadataPos = fileMetadataPos;
        this.fileMetadataSize = fileMetadataSize;
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readTailMagic() throws IOException {
        long totalSize = tsFileInput.size();
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        tsFileInput.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readHeadMagic() throws IOException {
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        tsFileInput.read(magicStringBytes, 0);
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public TsFileMetaData readFileMetadata() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(fileMetadataSize);
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), fileMetadataPos, buffer);
        buffer.flip();
        return TsFileMetaData.deserializeFrom(buffer);
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(index.getLen());
        tsFileInput.read(buffer, index.getOffset());
        buffer.flip();
        return TsDeviceMetadata.deserializeFrom(buffer);
    }

    public ChunkGroupFooter readChunkGroupFooter() throws IOException {
        return ChunkGroupFooter.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
    }

    /**
     * After reading the footer of a ChunkGroup, call this method to set the file pointer to the start of the data of this
     * ChunkGroup if you want to read its data next.
     */
    public void prepareReadChunkGroup(ChunkGroupFooter footer) throws IOException {
        tsFileInput.position(tsFileInput.position() - footer.getDataSize() - footer.getSerializedSize());
    }

    public ChunkHeader readChunkHeader() throws IOException {
        return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), true);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @param offset the file offset of this chunk's header
     */
    private ChunkHeader readChunkHeader(long offset) throws IOException {
        tsFileInput.position(offset);
        return ChunkHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), false);
    }

    /**
     * notice, the position of the channel MUST be at the end of this header.
     *
     * @return the pages of this chunk
     */
    private ByteBuffer readChunk(ChunkHeader header) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(header.getDataSize());
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), buffer);
        buffer.flip();
        return buffer;
    }

    /**
     * notice, this function will modify channel's position.
     */
    public Chunk readMemChunk(ChunkMetaData metaData) throws IOException {
        ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader());
        ByteBuffer buffer = ByteBuffer.allocate(header.getDataSize());
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), buffer);
        buffer.flip();
        return new Chunk(header, buffer);
    }

    /**
     * notice, the function will midify channel's position.<br>
     * notice, the target bytebuffer are not flipped.
     */
    public int readRaw(long position, int length, ByteBuffer target) throws IOException {
        tsFileInput.position(position);
        return ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), target, length);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @return the pages of this chunk
     */
    public ByteBuffer readChunk(ChunkHeader header, long positionOfChunkHeader) throws IOException {
        tsFileInput.position(positionOfChunkHeader);
        return readChunk(header);
    }

    public PageHeader readPageHeader(TSDataType type) throws IOException {
        return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @param offset the file offset of this page header's header
     */
    public PageHeader readPageHeader(TSDataType type, long offset) throws IOException {
        tsFileInput.position(offset);
        return PageHeader.deserializeFrom(tsFileInput.wrapAsInputStream(), type);
    }

    public long position() throws IOException {
        return tsFileInput.position();
    }

    public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(header.getCompressedSize());
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), buffer);
        buffer.flip();
        UnCompressor unCompressor = UnCompressor.getUnCompressor(type);
        ByteBuffer uncompressedBuffer = ByteBuffer.allocate(header.getUncompressedSize());
        //unCompressor.uncompress(buffer, uncompressedBuffer);
        //uncompressedBuffer.flip();
        switch (type) {
            case UNCOMPRESSED:
                return buffer;
            default:
                unCompressor.uncompress(buffer.array(), buffer.position(), buffer.remaining(), uncompressedBuffer.array(), 0);
                return uncompressedBuffer;
        }

    }

    public byte readMarker() throws IOException {
        markerBuffer.clear();
        ReadWriteIOUtils.readAsPossible(tsFileInput.wrapAsFileChannel(), markerBuffer);
        markerBuffer.flip();
        return markerBuffer.get();
    }


    public void close() throws IOException {
        this.tsFileInput.close();
    }

}
