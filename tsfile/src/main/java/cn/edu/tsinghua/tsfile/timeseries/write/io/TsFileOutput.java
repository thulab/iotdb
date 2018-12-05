package cn.edu.tsinghua.tsfile.timeseries.write.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public interface TsFileOutput {

    /**
     * Writes <code>b.length</code> bytes from the specified byte array
     * to this output at the current position.
     *
     * @param      b   the data.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(byte[] b) throws IOException;

    /**
     * Writes <code>b.remaining()</code> bytes from the specified byte array
     * to this output at the current position.
     *
     * @param      b   the data.
     * @exception  IOException  if an I/O error occurs.
     */
    public void write(ByteBuffer b) throws IOException;


    /**
     * gets the current position of the Output. This method is usually used for recording where the data is.
     * <br/> For example, if the Output is a fileOutputStream, then getPosition returns its file position.
     * @return  current position
     * @throws java.io.IOException  if an I/O error occurs.
     */
    public long getPosition() throws IOException;

    /**
     * close the output
     * @throws IOException  if an I/O error occurs.
     */
    public void close() throws IOException;

    /**
     * convert this TsFileOutput as a outputstream.
     * @return an output stream whose position is the same with this Output
     * @throws IOException  if an I/O error occurs.
     */
    public OutputStream wrapAsStream() throws IOException;

}
