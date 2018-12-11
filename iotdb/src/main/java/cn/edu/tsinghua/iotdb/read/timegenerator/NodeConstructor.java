package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.TimeValuePairReader;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBSeriesReader;
import cn.edu.tsinghua.tsfile.exception.write.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.read.expression.IBinaryExpression;
import cn.edu.tsinghua.tsfile.read.expression.IExpression;
import cn.edu.tsinghua.tsfile.read.expression.impl.SingleSeriesExpression;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.read.query.timegenerator.node.OrNode;

import java.io.IOException;

import static cn.edu.tsinghua.tsfile.read.expression.ExpressionType.*;

public class NodeConstructor {

  public NodeConstructor() {
  }

  public Node construct(IExpression expression) throws IOException, FileNodeManagerException {
    if (expression.getType() == SERIES) {
      return new LeafNode(this.generateSeriesReader((SingleSeriesExpression) expression));
    } else {
      Node leftChild;
      Node rightChild;
      if (expression.getType() == OR) {
        leftChild = this.construct(((IBinaryExpression) expression).getLeft());
        rightChild = this.construct(((IBinaryExpression) expression).getRight());
        return new OrNode(leftChild, rightChild);
      } else if (expression.getType() == AND) {
        leftChild = this.construct(((IBinaryExpression) expression).getLeft());
        rightChild = this.construct(((IBinaryExpression) expression).getRight());
        return new AndNode(leftChild, rightChild);
      } else {
        throw new UnSupportedDataTypeException("Unsupported QueryFilterType when construct OperatorNode: " + expression.getType());
      }
    }
  }

  public TimeValuePairReader generateSeriesReader(SingleSeriesExpression singleSeriesExpression)
          throws IOException, FileNodeManagerException {
    QueryDataSource queryDataSource = QueryDataSourceExecutor.getQueryDataSource(singleSeriesExpression);
    return new IoTDBSeriesReader(queryDataSource, singleSeriesExpression);
  }


}
