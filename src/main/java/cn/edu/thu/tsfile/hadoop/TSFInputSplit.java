package cn.edu.thu.tsfile.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author liukun
 */
public class TSFInputSplit extends InputSplit implements Writable{

    
	private Path path;
    private String deviceId;
    private int indexOfDeviceRowGroup;
    private long start;
    private long length;
    private String[] hosts;

    public TSFInputSplit() {

    }

    /**
     * @param path
     * @param deviceId
     * @param indexOfDeviceRowGroup
     * @param start
     * @param length
     * @param hosts
     */
    public TSFInputSplit(Path path, String deviceId, int indexOfDeviceRowGroup, long start, long length,
                         String[] hosts) {
        this.path = path;
        this.deviceId = deviceId;
        this.indexOfDeviceRowGroup = indexOfDeviceRowGroup;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
    }

    /**
     * @return the path
     */
    public Path getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(Path path) {
        this.path = path;
    }

    /**
     * @return the deviceId
     */
    public String getDeviceId() {
        return deviceId;
    }

    /**
     * @param deviceId the deviceId to set
     */
    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * @return the indexOfDeviceRowGroup
     */
    public int getIndexOfDeviceRowGroup() {
        return indexOfDeviceRowGroup;
    }

    /**
     * @param indexOfDeviceRowGroup the indexOfDeviceRowGroup to set
     */
    public void setIndexOfDeviceRowGroup(int indexOfDeviceRowGroup) {
        this.indexOfDeviceRowGroup = indexOfDeviceRowGroup;
    }

    /**
     * @return the start
     */
    public long getStart() {
        return start;
    }

    /**
     * @param start the start to set
     */
    public void setStart(long start) {
        this.start = start;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return this.hosts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(path.toString());
        out.writeUTF(deviceId);
        out.writeInt(indexOfDeviceRowGroup);
        out.writeLong(start);
        out.writeLong(length);
        out.writeInt(hosts.length);
        for (int i = 0; i < hosts.length; i++) {
            String string = hosts[i];
            out.writeUTF(string);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        path = new Path(in.readUTF());
        deviceId = in.readUTF();
        this.indexOfDeviceRowGroup = in.readInt();
        this.start = in.readLong();
        this.length = in.readLong();
        int len = in.readInt();
        this.hosts = new String[len];
        for (int i = 0; i < len; i++) {
            hosts[i] = in.readUTF();
        }
    }

	@Override
	public String toString() {
		return "TSFInputSplit [path=" + path + ", deviceId=" + deviceId + ", indexOfDeviceRowGroup="
				+ indexOfDeviceRowGroup + ", start=" + start + ", length=" + length + ", hosts="
				+ Arrays.toString(hosts) + "]";
	}


}
