package cn.edu.tsinghua.tsfile.file.metadata.statistics;

import cn.edu.tsinghua.tsfile.exception.TsFileRuntimeException;

public class StatisticsClassException extends TsFileRuntimeException {
    private static final long serialVersionUID = -5445795844780183770L;

    public StatisticsClassException(Class<?> className1, Class<?> className2) {
        super("tsfile-file Statistics classes mismatched: " + className1 + " vs. "
                + className2);
    }
}
