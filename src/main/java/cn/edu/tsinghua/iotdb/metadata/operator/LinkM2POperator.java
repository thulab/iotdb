package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LinkM2POperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.LINK_M2P;

    public String path;
    public String mPath;

    public LinkM2POperator(String fullPath, String fullPath1) {
        this.path = fullPath;
        this.mPath = fullPath1;
    }

    public LinkM2POperator() {

    }

    @Override
    public void writeTo(DataOutputStream os) throws IOException {
        os.writeInt(operatorType.ordinal());
        os.writeUTF(path);
        os.writeUTF(mPath);
    }

    @Override
    public void readFrom(DataInputStream is) throws IOException {
        path = is.readUTF();
        mPath = is.readUTF();
    }
}
