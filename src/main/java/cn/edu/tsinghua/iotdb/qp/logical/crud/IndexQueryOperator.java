package cn.edu.tsinghua.iotdb.qp.logical.crud;


import cn.edu.tsinghua.iotdb.index.IndexManager;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.iotdb.qp.constant.SQLConstant;
import cn.edu.tsinghua.iotdb.qp.logical.index.KvMatchIndexQueryOperator;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import static cn.edu.tsinghua.iotdb.index.IndexManager.*;

public class IndexQueryOperator extends SFWOperator {

	private final IndexType indexType;
	protected Path path;

	public IndexQueryOperator(int tokenIntType, IndexType indexType) {
		super(tokenIntType);
		this.operatorType = OperatorType.INDEXQUERY;
		this.indexType = indexType;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public static IndexQueryOperator getIndexQueryOperator(String indexTypeString) throws IndexManagerException {
		switch (IndexType.getIndexType(indexTypeString)){
			case KvIndex:
				return new KvMatchIndexQueryOperator(SQLConstant.TOK_QUERY_INDEX);
			default:
				throw new IndexManagerException("unsupport index type:" + indexTypeString);

		}
	}
}
