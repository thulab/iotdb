package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AddPTreeOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.ADD_PTREE;

    public String rootName;

    public AddPTreeOperator(String fullPath) {
        this.rootName = fullPath;
    }

    public AddPTreeOperator() {

    }

    @Override
    public void writeTo(DataOutputStream os) throws IOException {
        os.writeInt(operatorType.ordinal());
        os.writeUTF(rootName);
    }

    @Override
    public void readFrom(DataInputStream is) throws IOException {
        rootName = is.readUTF();
    }
}
