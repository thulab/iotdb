package cn.edu.tsinghua.tsfile.read;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
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
    private Path path;
    private FileChannel channel;
    private long fileMetadataPos;
    private int fileMetadataSize;
    private ByteBuffer markerBuffer = ByteBuffer.allocate(Byte.BYTES);

    public TsFileSequenceReader(String file) throws IOException {
        this.path = Paths.get(file);
        open();
    }

    /**
     * After open the file, the reader position is at the end of the  magic string in the header.
     */
    private void open() throws IOException {
        channel = FileChannel.open(path, StandardOpenOption.READ);
        ByteBuffer metadataSize = ByteBuffer.allocate(Integer.BYTES);
        channel.read(metadataSize, channel.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES);
        metadataSize.flip();
        fileMetadataSize = ReadWriteIOUtils.readInt(metadataSize);//read file metadata size and position
        fileMetadataPos = channel.size() - TSFileConfig.MAGIC_STRING.length() - Integer.BYTES - fileMetadataSize;
        channel.position(TSFileConfig.MAGIC_STRING.length());//skip the magic header
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readTailMagic() throws IOException {
        long totalSize = channel.size();
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        channel.read(magicStringBytes, totalSize - TSFileConfig.MAGIC_STRING.length());
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public String readHeadMagic() throws IOException {
        ByteBuffer magicStringBytes = ByteBuffer.allocate(TSFileConfig.MAGIC_STRING.length());
        channel.read(magicStringBytes, 0);
        magicStringBytes.flip();
        return new String(magicStringBytes.array());
    }

    /**
     * this function does not modify the position of the file reader.
     */
    public TsFileMetaData readFileMetadata() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(fileMetadataSize);
        ReadWriteIOUtils.readAsPossible(channel, fileMetadataPos, buffer);
        buffer.flip();
        return TsFileMetaData.deserializeFrom(buffer);
    }


    public TsDeviceMetadata readTsDeviceMetaData(TsDeviceMetadataIndex index) throws IOException {
        channel.position(index.getOffset());
        ByteBuffer buffer = ByteBuffer.allocate(index.getLen());
        channel.read(buffer);
        buffer.flip();
        return TsDeviceMetadata.deserializeFrom(buffer);
    }

    public ChunkGroupFooter readChunkGroupFooter() throws IOException {
        return ChunkGroupFooter.deserializeFrom(Channels.newInputStream(channel), true);
    }

    /**
     * After reading the footer of a ChunkGroup, call this method to set the file pointer to the start of the data of this
     * ChunkGroup if you want to read its data next.
     */
    public void prepareReadChunkGroup(ChunkGroupFooter footer) throws IOException {
        channel.position(channel.position() - footer.getDataSize() - footer.getSerializedSize());
    }

    public ChunkHeader readChunkHeader() throws IOException {
        return ChunkHeader.deserializeFrom(Channels.newInputStream(channel), true);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @param offset the file offset of this chunk's header
     */
    public ChunkHeader readChunkHeader(long offset) throws IOException {
        channel.position(offset);
        return ChunkHeader.deserializeFrom(Channels.newInputStream(channel), false);
    }

    /**
     * notice, the position of the channel MUST be at the end of this header.
     *
     * @return the pages of this chunk
     */
    public ByteBuffer readChunk(ChunkHeader header) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(header.getDataSize());
        ReadWriteIOUtils.readAsPossible(channel, buffer);
        buffer.flip();
        return buffer;
    }

    /**
     * notice, this function will modify channel's position.
     */
    public Chunk readMemChunk(ChunkMetaData metaData) throws IOException {
        ChunkHeader header = readChunkHeader(metaData.getOffsetOfChunkHeader());
        ByteBuffer buffer = ByteBuffer.allocate(header.getDataSize());
        ReadWriteIOUtils.readAsPossible(channel, buffer);
        buffer.flip();
        return new Chunk(header, buffer);
    }

    /**
     * notice, the function will midify channel's position.
     */
    public int readRaw(long position, int length, ByteBuffer output) throws IOException {
        channel.position(position);
        return ReadWriteIOUtils.readAsPossible(channel, output, length);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @return the pages of this chunk
     */
    public ByteBuffer readChunk(ChunkHeader header, long positionOfChunkHeader) throws IOException {
        channel.position(positionOfChunkHeader);
        return readChunk(header);
    }

    public PageHeader readPageHeader(TSDataType type) throws IOException {
        return PageHeader.deserializeFrom(Channels.newInputStream(channel), type);
    }

    /**
     * notice, this function will modify channel's position.
     *
     * @param offset the file offset of this page header's header
     */
    public PageHeader readPageHeader(TSDataType type, long offset) throws IOException {
        channel.position(offset);
        return PageHeader.deserializeFrom(Channels.newInputStream(channel), type);
    }

    public FileChannel getChannel() {
        return channel;
    }

    public ByteBuffer readPage(PageHeader header, CompressionType type) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(header.getCompressedSize());
        ReadWriteIOUtils.readAsPossible(channel, buffer);
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
        ReadWriteIOUtils.readAsPossible(channel, markerBuffer);
        markerBuffer.flip();
        return markerBuffer.get();
    }


    public void close() throws IOException {
        this.channel.close();
    }

}
