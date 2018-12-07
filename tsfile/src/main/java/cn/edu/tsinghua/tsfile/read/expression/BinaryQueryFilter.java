package cn.edu.tsinghua.tsfile.read.expression;

/**
 * @author Jinrui Zhang
 */
public interface BinaryQueryFilter extends QueryFilter{
    QueryFilter getLeft();

    QueryFilter getRight();


}
