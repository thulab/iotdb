package cn.edu.tsinghua.iotdb.metadata.operator;

import cn.edu.tsinghua.iotdb.index.IndexManager;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AddIndexOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.ADD_INDEX;

    public String path;
    public IndexManager.IndexType indexType;

    public AddIndexOperator(String path, IndexManager.IndexType indexType) {
        this.path = path;
        this.indexType = indexType;
    }

    public AddIndexOperator() {

    }

    @Override
    public void writeTo(DataOutputStream os) throws IOException {
        os.writeInt(operatorType.ordinal());
        os.writeUTF(path);
        os.writeInt(indexType.ordinal());
    }

    @Override
    public void readFrom(DataInputStream is) throws IOException {
        path = is.readUTF();
        indexType = IndexManager.IndexType.values()[is.readInt()];
    }
}
