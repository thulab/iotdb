package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DeletePathOperator implements MetaOperator {

    public static final OperatorType operatorType = OperatorType.DELETE_PATH;
    public String path;

    public DeletePathOperator(String p) {
        this.path = p;
    }

    public DeletePathOperator() {

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
