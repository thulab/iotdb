package cn.edu.thu.tsfile.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test the {@link com.corp.tsfile.hadoop.TSFInputSplit}
 * Assert the readFields function and write function is right
 *
 * @author hadoop
 */
public class TSFInputSplitTest {

    private TSFInputSplit wInputSplit;
    private TSFInputSplit rInputSplit;
    private DataInputBuffer DataInputBuffer = new DataInputBuffer();
    private DataOutputBuffer DataOutputBuffer = new DataOutputBuffer();

    @Before
    public void setUp() throws Exception {
        // For the test data
        Path path = new Path("input");
        String deviceId = "d1";
        int numOfRowGroupMetaDate = 1;
        long start = 0;
        long length = 100;
        String[] hosts = {"192.168.1.1", "192.168.1.0", "localhost"};

        wInputSplit = new TSFInputSplit(path, deviceId, numOfRowGroupMetaDate, start, length, hosts);
        rInputSplit = new TSFInputSplit();
    }

    @Test
    public void testInputSplitWriteAndRead() {
        try {
            // call the write method to serialize the object
            wInputSplit.write(DataOutputBuffer);
            DataOutputBuffer.flush();
            DataInputBuffer.reset(DataOutputBuffer.getData(), DataOutputBuffer.getLength());
            rInputSplit.readFields(DataInputBuffer);
            DataInputBuffer.close();
            DataOutputBuffer.close();
            // assert
            assertEquals(wInputSplit.getPath(), rInputSplit.getPath());
            assertEquals(wInputSplit.getDeviceId(), rInputSplit.getDeviceId());
            assertEquals(wInputSplit.getIndexOfDeviceRowGroup(), rInputSplit.getIndexOfDeviceRowGroup());
            assertEquals(wInputSplit.getStart(), rInputSplit.getStart());
            try {
                assertEquals(wInputSplit.getLength(), rInputSplit.getLength());
                assertArrayEquals(wInputSplit.getLocations(), rInputSplit.getLocations());
            } catch (InterruptedException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
