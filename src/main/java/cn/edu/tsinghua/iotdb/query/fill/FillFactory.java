package cn.edu.tsinghua.iotdb.query.fill;

import cn.edu.tsinghua.iotdb.exception.UnSupportedFillTypeException;

import java.util.List;
import java.util.Set;


public class FillFactory {

    private Set<String> previousFillSupportType;
    private Set<String> linearFillSupportType;

    private FillFactory () {
        previousFillSupportType.add("boolean");
        previousFillSupportType.add("int32");
        previousFillSupportType.add("int64");
        previousFillSupportType.add("float");
        previousFillSupportType.add("double");
        previousFillSupportType.add("text");

        linearFillSupportType.add("int32");
        linearFillSupportType.add("int64");
        linearFillSupportType.add("float");
        linearFillSupportType.add("double");
    }

    private static class SingletonHolder {
        private static final FillFactory INSTANCE = new FillFactory();
    }

    public static final FillFactory getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public List<FillFactory> constructPreviousFill(String dataType, long time) {
        if (!previousFillSupportType.contains(dataType)) {
            throw new UnSupportedFillTypeException("previous fill does not support " + dataType);
        }

        return null;
    }

    public List<FillFactory> constructLinearFill(String dataType, long startTime, long endTime) {
        if (!linearFillSupportType.contains(dataType)) {
            throw new UnSupportedFillTypeException("linear fill does not support " + dataType);
        }

        return null;
    }

}
