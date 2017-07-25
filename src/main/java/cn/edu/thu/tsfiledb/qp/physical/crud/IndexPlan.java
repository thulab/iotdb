package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.crud.IndexOperator.IndexType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class IndexPlan extends PhysicalPlan {

	private Path path;
	private Map<String, Integer> parameters;
	private long startTime;
	private final IndexType indexType;

	public IndexPlan(Path path, Map<String, Integer> parameters,long startTime,IndexType indexType) {
		super(false, OperatorType.INDEX);
		this.path = path;
		this.indexType = indexType;
		this.parameters = parameters;
		this.startTime = startTime;
	}
	
	public IndexType getIndexType(){
		return indexType;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> list = new ArrayList<>();
		if(path!=null){
			list.add(path);
		}
		return list;
	}

	public Map<String, Integer> getParameters() {
		return parameters;
	}

	public long getStartTime() {
		return startTime;
	}
}
