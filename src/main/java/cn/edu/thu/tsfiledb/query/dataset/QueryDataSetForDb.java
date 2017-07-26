package cn.edu.thu.tsfiledb.query.dataset;

import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;

import java.io.ByteArrayInputStream;

/**
 * This class is used only in query process in TsFileDb,
 * it storage the structure consist of bufferWritePageList, insertTrue, updateTrue, updateFalse, deleteFilter.<br>
 *
 */
public class QueryDataSetForDb extends QueryDataSet{
    private InsertDynamicData deltaData;

    public InsertDynamicData getDeltaData() {
        return this.deltaData;
    }

    public void setDeltaData(InsertDynamicData deltaData) {
        this.deltaData = deltaData;
    }

}
