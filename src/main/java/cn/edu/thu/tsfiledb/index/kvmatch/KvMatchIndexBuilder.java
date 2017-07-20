package cn.edu.thu.tsfiledb.index.kvmatch;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.index.DataFileInfo;
import cn.edu.thu.tsfiledb.query.engine.OverflowQueryEngine;

import java.io.IOException;
import java.util.Arrays;

/**
 * This is the class actually build the KV-match index for specific data series.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexBuilder {

    private Path columnPath;
    private DataFileInfo fileInfo;
    private boolean overwrite;  // whether overwrite old index file (used for merge/close process)
    private OverflowQueryEngine queryEngine;
    private String indexFileDir;

    public KvMatchIndexBuilder(Path columnPath, DataFileInfo fileInfo, boolean overwrite) {
        this.columnPath = columnPath;
        this.fileInfo = fileInfo;
        this.overwrite = overwrite;
        queryEngine = new OverflowQueryEngine();
        indexFileDir = TsfileDBDescriptor.getInstance().getConfig().indexFileDir;
    }

    public boolean build() throws ProcessorException, PathErrorException, IOException {
        // Step 1: scan data and extract window features
        QueryDataSet dataSet = queryEngine.query(columnPath, fileInfo.getTimeInterval());

        double ex = 0;
        while (dataSet.next()) {
            RowRecord row = dataSet.getCurrentRecord();
            System.out.println(row.getTime() + "," + Arrays.toString(row.getFields().toArray()));

        }

        // Step 2: make up index structure

        // Step 3: store to disk
        if (overwrite) {

        } else {

        }

        return false;
    }
}
