package org.apache.iotdb.tsfile.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.read.reader.TsFileInput;

public class HDFSInputStream implements TsFileInput {

  private FSDataInputStream fsDataInputStream;
  private FileStatus fileStatus;

  public HDFSInputStream(String filePath) throws IOException {

    this(filePath, new Configuration());
  }

  public HDFSInputStream(String filePath, Configuration configuration) throws IOException {

    this(new Path(filePath), configuration);
  }

  public HDFSInputStream(Path path, Configuration configuration) throws IOException {
    FileSystem fs = FileSystem.get(configuration);
    fsDataInputStream = fs.open(path);
    fileStatus = fs.getFileStatus(path);
  }

  @Override
  public long size() {
    return fileStatus.getLen(); //TODO
  }

  @Override
  public long position() throws IOException {
    return fsDataInputStream.getPos();
  }

  @Override
  public TsFileInput position(long newPosition) throws IOException {
    fsDataInputStream.seek(newPosition); //TODO
    return this;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    byte[] bytes = new byte[dst.capacity()];
    int res = fsDataInputStream.read(bytes);
    dst.put(bytes);
    return res;
  }

  @Override
  public int read(ByteBuffer dst, long position) throws IOException {
    long srcPosition = fsDataInputStream.getPos();

    fsDataInputStream.seek(position);
    byte[] bytes = new byte[dst.capacity()];
    int res = fsDataInputStream.read(bytes);
    dst.put(bytes);

    fsDataInputStream.seek(srcPosition);//TODO This method does not modify this TsFileInput's position.
    return res;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public FileChannel wrapAsFileChannel() throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public InputStream wrapAsInputStream() throws IOException {
    return fsDataInputStream;
  }

  @Override
  public void close() throws IOException {
    fsDataInputStream.close();
  }

  @Override
  public int readInt() throws IOException {
    throw new IOException("Not support");
  }

}
