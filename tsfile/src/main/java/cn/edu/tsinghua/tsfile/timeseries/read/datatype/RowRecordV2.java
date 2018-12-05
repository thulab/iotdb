package cn.edu.tsinghua.tsfile.timeseries.read.datatype;

import java.util.ArrayList;
import java.util.List;

public class RowRecordV2 {
    private long timestamp;
    private List<Field> fields;

    public RowRecordV2(long timestamp) {
        this.timestamp = timestamp;
        this.fields = new ArrayList<>();
    }

    public long getTime() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void addField(Field f) {
        this.fields.add(f);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);
        for (Field f : fields) {
            sb.append("\t");
            sb.append(f);
        }
        return sb.toString();
    }
    public long getTimestamp() {
        return timestamp;
    }

    public List<Field> getFields() {
        return fields;
    }
}
