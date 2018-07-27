package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class DeletePTreePathOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.DELETE_PTREE_PATH;

    public String path;

    public DeletePTreePathOperator(String fullPath) {
        this.path = fullPath;
    }

    public DeletePTreePathOperator() {

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
