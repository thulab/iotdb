package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.timeseries.filter.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.filter.operator.NoRestriction;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

public class QueryDataSourceExecutor {
    private static FileNodeManager fileNodeManager = FileNodeManager.getInstance();

    public static QueryDataSource getQueryDataSource(SeriesFilter<?> seriesFilter) throws FileNodeManagerException {
        return fileNodeManager.query(seriesFilter);
    }

    public static QueryDataSource getQueryDataSource(Path selectedPath) throws FileNodeManagerException {
        SeriesFilter seriesFilter = new SeriesFilter(selectedPath, NoRestriction.getInstance());
        return fileNodeManager.query(seriesFilter);
    }
}
