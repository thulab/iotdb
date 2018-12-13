package cn.edu.tsinghua.tsfile.read.reader;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class DefaultTsFileInput implements TsFileInput {
    FileChannel channel;

    public DefaultTsFileInput(Path file) throws IOException {
        channel = FileChannel.open(file, StandardOpenOption.READ);
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public TsFileInput position(long newPosition) throws IOException {
        channel.position(newPosition);
        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public FileChannel wrapAsFileChannel() throws IOException {
        return channel;
    }

    @Override
    public InputStream wrapAsInputStream() throws IOException {
        return Channels.newInputStream(channel);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
