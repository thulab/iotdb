package cn.edu.thu.tsfiledb.engine.filenode;

import java.util.List;

import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.readSupport.RowRecord;

public class QueryerForMerge {

	public QueryerForMerge(List<Path> pathList, SingleSeriesFilterExpression timeFilter) {
		// TODO Auto-generated constructor stub
	}

	public boolean hasNextRecord() {
		// TODO Auto-generated method stub
		return false;
	}

	public RowRecord getNextRecord() {
		// TODO Auto-generated method stub
		return null;
	}

}
