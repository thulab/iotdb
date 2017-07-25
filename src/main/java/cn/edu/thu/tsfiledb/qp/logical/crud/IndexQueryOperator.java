package cn.edu.thu.tsfiledb.qp.logical.crud;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;

public class IndexQueryOperator extends SFWOperator {

	private Path path;
	private String csvPath;
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

	public void setPath(Path path) {
		this.path = path;
	}

	public String getCsvPath() {
		return csvPath;
	}

	public void setCsvPath(String csvPath) {
		this.csvPath = csvPath;
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
