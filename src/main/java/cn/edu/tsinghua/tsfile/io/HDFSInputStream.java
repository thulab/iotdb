package cn.edu.tsinghua.tsfile.io;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * This class is used to wrap the {@link}FSDataInputStream and implement the
 * interface {@link}TSRandomAccessFileReader.
 *
 */
public class HDFSInputStream implements ITsRandomAccessFileReader {

	private final FSDataInputStream fsDataInputStream;
	private final long length;

	public HDFSInputStream(String filePath) throws IOException {

		this(filePath, new Configuration());
	}

	public HDFSInputStream(String filePath, Configuration configuration) throws IOException {

		this(new Path(filePath),configuration);
	}

	public HDFSInputStream(Path path, Configuration conf) throws IOException {
		length = path.getFileSystem(conf).getFileStatus(path).getLen();
		fsDataInputStream = path.getFileSystem(conf).open(path);
	}

	public void seek(long offset) throws IOException {

		fsDataInputStream.seek(offset);
	}

	public int read() throws IOException {

		return fsDataInputStream.read();
	}

	public long length() throws IOException {

		return length;
	}

	public int readInt() throws IOException {

		return fsDataInputStream.readInt();
	}

	public void close() throws IOException {

		fsDataInputStream.close();
	}

	public long getPos() throws IOException {

		return fsDataInputStream.getPos();
	}

	/**
	 * Read the data into b, and the check the length
	 *
	 * @param b
	 *            read the data into
	 * @param off
	 *            the begin offset to read into
	 * @param len
	 *            the length to read into
	 */
	public int read(byte[] b, int off, int len) throws IOException {
		if (len < 0) {
			throw new IndexOutOfBoundsException();
		}
		int n = 0;
		while (n < len) {
			int count = fsDataInputStream.read(b, off + n, len - n);
			if (count < 0) {
				throw new IOException("The read length is out of the length of inputstream");
			}
			n += count;
		}
		return n;
	}
}
