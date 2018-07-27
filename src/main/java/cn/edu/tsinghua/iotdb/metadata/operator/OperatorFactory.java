package cn.edu.tsinghua.iotdb.metadata.operator;

import java.io.DataInputStream;
import java.io.IOException;

public class OperatorFactory {

    public static MetaOperator create(int operatorTypeIdx) {
        return create(OperatorType.values()[operatorTypeIdx]);
    }


    public static MetaOperator create(OperatorType operatorType) {
        MetaOperator operator = null;
        switch (operatorType) {
            case ADD_PATH:
                operator = new AddPathOperator();
                break;
            case LINK_M2P:
                operator = new LinkM2POperator();
                break;
            case ADD_INDEX:
                operator = new AddIndexOperator();
                break;
            case ADD_PTREE:
                operator = new AddPTreeOperator();
                break;
            case UNLINK_M2P:
                operator = new UnlinkM2POperator();
                break;
            case DELETE_PATH:
                operator = new DeletePathOperator();
                break;
            case SET_STORAGE:
                operator = new SetStorageOperator();
                break;
            case DELETE_INDEX:
                operator = new DeleteIndexOperator();
                break;
            case ADD_PTREE_PATH:
                operator = new AddPTreePathOperator();
                break;
            case DELETE_PTREE_PATH:
                operator = new DeletePTreePathOperator();
                break;
        }
        return operator;
    }

    public static MetaOperator readFromStream(DataInputStream is) throws IOException {
        if (is.available() < 4)
            return null;
        int operatorTypeIdx = is.readInt();
        MetaOperator operator = create(operatorTypeIdx);
        if (operator != null)
            operator.readFrom(is);
        return operator;
    }
}
