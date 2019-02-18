package org.apache.iotdb.tsfile.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;


/**
 * This class is used to wrap the {@link}FSDataOutputStream and implement the interface
 * {@link}TsFileOutput
 */
public class HDFSOutputStream implements TsFileOutput {

  private FSDataOutputStream fsDataOutputStream;

  public HDFSOutputStream(String filePath, boolean overwriter) throws IOException {

    this(filePath, new Configuration(), overwriter);
  }


  public HDFSOutputStream(String filePath, Configuration configuration, boolean overwriter)
      throws IOException {

    this(new Path(filePath), configuration, overwriter);
  }

  public HDFSOutputStream(Path path, Configuration configuration, boolean overwriter)
      throws IOException {

    FileSystem fsFileSystem = FileSystem.get(configuration);
    fsDataOutputStream = fsFileSystem.create(path, overwriter);
  }

  @Override
  public void write(byte[] b) throws IOException {

    fsDataOutputStream.write(b);
  }

  public void write(ByteBuffer b) throws IOException {
    throw new IOException("Not support");
  }

  @Override
  public long getPosition() throws IOException {

    return fsDataOutputStream.getPos();
  }

  @Override
  public void close() throws IOException {

    fsDataOutputStream.close();
  }

  @Override
  public OutputStream wrapAsStream() throws IOException {
    return fsDataOutputStream;
  }

  @Override
  public void flush() throws IOException {
    this.fsDataOutputStream.flush();
  }

  @Override
  public void truncate(long position) throws IOException {
    throw new IOException("Not support");
  }
}
