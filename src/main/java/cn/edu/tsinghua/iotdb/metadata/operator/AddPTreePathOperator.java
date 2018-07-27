package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AddPTreePathOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.ADD_PTREE_PATH;

    public String path;

    public AddPTreePathOperator(String fullPath) {
        this.path = fullPath;
    }

    public AddPTreePathOperator() {

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
