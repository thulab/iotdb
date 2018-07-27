package cn.edu.tsinghua.iotdb.metadata.operator;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class AddPathOperator implements MetaOperator {
    public static final OperatorType operatorType = OperatorType.ADD_PATH;

    public String path;
    public TSDataType dataType;
    public TSEncoding encoding;
    public String[] args;

    public AddPathOperator(String path, TSDataType dataType, TSEncoding encoding, String[] args) {
        this.path = path;
        this.dataType = dataType;
        this.encoding = encoding;
        this.args = args;
    }

    public AddPathOperator() {

    }

    @Override
    public void writeTo(DataOutputStream os) throws IOException {
        os.writeInt(operatorType.ordinal());
        os.writeUTF(path);
        os.writeInt(dataType.ordinal());
        os.writeInt(encoding.ordinal());
        if (args != null) {
            os.writeInt(args.length);
            for (String arg : args)
                os.writeUTF(arg);
        } else {
            os.writeInt(0);
        }
    }

    @Override
    public void readFrom(DataInputStream is) throws IOException {
        path = is.readUTF();
        dataType = TSDataType.values()[is.readInt()];
        encoding = TSEncoding.values()[is.readInt()];
        int argc = is.readInt();
        if (argc > 0) {
            args = new String[argc];
            for (int i = 0; i < argc; i++)
                args[i] = is.readUTF();
        }
    }
}
