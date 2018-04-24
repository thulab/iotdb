package cn.edu.tsinghua.iotdb.engine.tombstone;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tombstone tombstone = (Tombstone) o;
        return deleteTimestamp == tombstone.deleteTimestamp &&
                executeTimestamp == tombstone.executeTimestamp &&
                Objects.equals(deltaObjectId, tombstone.deltaObjectId) &&
                Objects.equals(measurementId, tombstone.measurementId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(deltaObjectId, measurementId, deleteTimestamp, executeTimestamp);
    }
}
