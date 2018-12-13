package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.tsfile.read.common.Path;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;

/**
 * <p> This class is used to get query data source of a given path.
 * See the component of <code>QueryDataSource</code>
 */
public class QueryDataSourceManager {

    private static FileNodeManager fileNodeManager = FileNodeManager.getInstance();

    public static QueryDataSource getQueryDataSource(Path selectedPath) throws FileNodeManagerException {

        // TODO use null to replace NoRestriction filter operator

        SingleSeriesExpression singleSeriesExpression = new SingleSeriesExpression(selectedPath, null);
        return fileNodeManager.query(singleSeriesExpression);
    }
}
