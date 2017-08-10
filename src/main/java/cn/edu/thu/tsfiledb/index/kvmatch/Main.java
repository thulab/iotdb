package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;
import cn.edu.thu.tsfiledb.index.common.DataFileInfo;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;

import java.io.IOException;
import java.util.List;

public class Main {

    public static void main(String[] args) throws FileNodeManagerException, IOException {

        Path columnPath = new Path("root.turbine.Beijing.d3.Speed");
        OverflowQueryEngine overflowQueryEngine = new OverflowQueryEngine();

        // 1. get information of all files containing this column path.
        List<DataFileInfo> fileInfoList = FileNodeManager.getInstance().indexBuildQuery(columnPath, 0);

        // 2. build index for every data file.
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                System.out.println(columnPath);
                for (DataFileInfo fileInfo : fileInfoList) {
                    QueryDataSet dataSet = null;
                    try {
                        dataSet = overflowQueryEngine.getDataInTsFile(columnPath, fileInfo.getFilePath());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    while (dataSet.next()) {
                        //System.out.println(columnPath);
                    }
                }
            }).start();
        }
    }
}
