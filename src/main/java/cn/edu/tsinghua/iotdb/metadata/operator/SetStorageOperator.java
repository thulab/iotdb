package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SetStorageOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.SET_STORAGE;

    public String path;

    public SetStorageOperator(String fullPath) {
        this.path = fullPath;
    }

    public SetStorageOperator() {

    }

    @Override
    public void writeTo(DataOutputStream os) throws IOException {
        os.writeInt(operatorType.ordinal());
        os.writeUTF(path);
    }

    @Override
    public void readFrom(DataInputStream is) throws IOException {
        path = is.readUTF();
    }
}
