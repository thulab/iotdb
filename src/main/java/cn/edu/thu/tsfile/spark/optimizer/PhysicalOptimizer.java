package cn.edu.thu.tsfile.spark.optimizer;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.spark.common.BasicOperator;
import cn.edu.thu.tsfile.spark.common.FilterOperator;
import cn.edu.thu.tsfile.spark.common.SingleQuery;
import cn.edu.thu.tsfile.spark.common.TSQueryPlan;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by qiaojialin on 2017/4/27.
 */
public class PhysicalOptimizer {

    //determine whether to query all delta_objects from TSFile. true means do query.
    private boolean flag;

    public List<TSQueryPlan> optimize(SingleQuery singleQuery, List<String> paths, TSRandomAccessFileReader in, Long start, Long end) throws IOException {

        QueryEngine queryEngine = new QueryEngine(in);
        List<String> actualDeltaObjects = queryEngine.getAllDeltaObjectUIDByPartition(start, end);
        List<SeriesSchema> actualSeries = queryEngine.getAllSeries();

        List<String> validDeltaObjects = new ArrayList<>();

        List<String> selectedSeries = new ArrayList<>();
        for(String path: paths) {
            if(!path.equals(SQLConstant.RESERVED_DELTA_OBJECT) && !path.equals(SQLConstant.RESERVED_TIME)) {
                selectedSeries.add(path);
            }
        }
        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;

        if(singleQuery != null) {
            timeFilter = singleQuery.getTimeFilterOperator();
            valueFilter = singleQuery.getValueFilterOperator();
            if (valueFilter != null) {
                List<String> filterPaths = valueFilter.getAllPaths();
                List<String> actualPaths = new ArrayList<>();
                for (SeriesSchema series : actualSeries) {
                    actualPaths.add(series.name);
                }
                //if filter paths doesn't in tsfile, don't query
                if (!actualPaths.containsAll(filterPaths))
                    return new ArrayList<>();
            }

            flag = true;
            Set<String> selectDeltaObjects = mergeDeltaObject(singleQuery.getDeltaObjectFilterOperator());
            if(!flag) {
                //e.g. where delta_object = 'd1' and delta_object = 'd2', should not query
                return new ArrayList<>();
            }

            //if select deltaObject, then match with measurement
            if(selectDeltaObjects != null && !selectDeltaObjects.isEmpty()) {
                //check whether selected deltaObjects belong to file
                for(String deltaObject: selectDeltaObjects) {
                    if(actualDeltaObjects.contains(deltaObject)) {
                        validDeltaObjects.add(deltaObject);
                    }
                }
            } else {
                validDeltaObjects.addAll(queryEngine.getAllDeltaObjectUIDByPartition(start, end));
            }
        } else {
            validDeltaObjects.addAll(queryEngine.getAllDeltaObjectUIDByPartition(start, end));
        }

        List<SeriesSchema> fileSeries = queryEngine.getAllSeries();
        Set<String> seriesSet = new HashSet<>();
        for(SeriesSchema series: fileSeries) {
            seriesSet.add(series.name);
        }

        //query all measurements from TSFile
        if(selectedSeries.size() == 0) {
            for(SeriesSchema series: actualSeries) {
                selectedSeries.add(series.name);
            }
        } else {
            //remove paths that doesn't exist in file
            selectedSeries.removeIf(path -> !seriesSet.contains(path));
        }


        List<TSQueryPlan> tsFileQueries = new ArrayList<>();
        for(String deltaObject: validDeltaObjects) {
            List<String> newPaths = new ArrayList<>();
            for(String path: selectedSeries) {
                String newPath = deltaObject + SQLConstant.PATH_SEPARATOR + path;
                newPaths.add(newPath);
            }
            if(valueFilter == null) {
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, null));
            } else {
                FilterOperator newValueFilter = valueFilter.clone();
                newValueFilter.addHeadDeltaObjectPath(deltaObject);
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, newValueFilter));
            }
        }
        return tsFileQueries;
    }


    private Set<String> mergeDeltaObject(FilterOperator deltaFilterOperator) {
        if (deltaFilterOperator == null) {
            return null;
        }
        if (deltaFilterOperator.isLeaf()) {
            Set<String> r = new HashSet<>();
            r.add(((BasicOperator)deltaFilterOperator).getSeriesValue());
            return r;
        }
        List<FilterOperator> children = deltaFilterOperator.getChildren();
        if (children == null || children.isEmpty()) {
            return new HashSet<>();
        }
        Set<String> ret = mergeDeltaObject(children.get(0));
        if(ret == null){
            return null;
        }
        for (int i = 1; i < children.size(); i++) {
            Set<String> temp = mergeDeltaObject(children.get(i));
            if(temp == null) {
                return null;
            }
            switch (deltaFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.retainAll(temp);
                    //example: "where delta_object = d1 and delta_object = d2" should not query data
                    if(ret.isEmpty()) {
                        flag = false;
                    }
                    break;
                case SQLConstant.KW_OR:
                    ret.addAll(temp);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:"+deltaFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }
}
