package cn.edu.tsinghua.iotdb.engine.tombstone;

public class Tombstone {
    public String seriesName;
    public long deleteTimestamp;
    public long executeTimestamp;


    public Tombstone(String seriesName, long timestamp, long executeTimestamp) {
        this.seriesName = seriesName;
        this.deleteTimestamp = timestamp;
        this.executeTimestamp = executeTimestamp;
    }
}
