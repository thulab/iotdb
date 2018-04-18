package cn.edu.tsinghua.iotdb.engine.tombstone;

public class Tombstone {
    public String deltaObjectId;
    public String measurementId;
    public long deleteTimestamp;
    public long executeTimestamp;


    public Tombstone(String deltaObjectId, String measurementId, long timestamp, long executeTimestamp) {
        this.deltaObjectId = deltaObjectId;
        this.measurementId = measurementId;
        this.deleteTimestamp = timestamp;
        this.executeTimestamp = executeTimestamp;
    }
}
