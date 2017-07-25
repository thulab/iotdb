package cn.edu.thu.tsfiledb.qp.logical.crud;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;

public class IndexQueryOperator extends SFWOperator {

	private Path path;
	private Path patternPath;
	private long startTime;
	private long endTime;
	private double epsilon;
	private double alpha;
	private double beta;
	private boolean hasParameter;
	
	public IndexQueryOperator(int tokenIntType) {
		super(tokenIntType);
		operatorType = OperatorType.INDEXQUERY;
	}

	public Path getPath() {
		return path;
	}

	public Path getPatternPath() {
		return patternPath;
	}

	public void setPatternPath(Path patternPath) {
		this.patternPath = patternPath;
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

	public void setPath(Path path) {
		this.path = path;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
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
}
