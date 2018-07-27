package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface MetaOperator {
    void writeTo(DataOutputStream os) throws IOException;

    void readFrom(DataInputStream is) throws IOException;
}
