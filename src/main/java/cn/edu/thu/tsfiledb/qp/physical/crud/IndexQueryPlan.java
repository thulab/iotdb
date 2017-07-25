package cn.edu.thu.tsfiledb.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;

public class IndexQueryPlan extends PhysicalPlan {
	
	private String csvPath;
	private double epsilon;
	private double alpha;
	private double beta;
	private boolean hasParameter;
	private List<Path> paths;
	private long startTime;
	private long endTime;
	public IndexQueryPlan(Path path,String csvPath,double epsilon) {
		super(true, OperatorType.INDEXQUERY);
		paths = new ArrayList<>();
		paths.add(path);
		this.csvPath = csvPath;
		this.epsilon = epsilon;
	}

	@Override
	public List<Path> getPaths() {
		return paths;
	}

	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public double getBeta() {
		return beta;
	}

	public void setBeta(double beta) {
		this.beta = beta;
	}

	public boolean isHasParameter() {
		return hasParameter;
	}

	public void setHasParameter(boolean hasParameter) {
		this.hasParameter = hasParameter;
	}

	public String getCsvPath() {
		return csvPath;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
}
