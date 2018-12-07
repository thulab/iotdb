package cn.edu.tsinghua.tsfile.read.expression;

import cn.edu.tsinghua.tsfile.read.filter.basic.Filter;

public interface UnaryQueryFilter extends QueryFilter{
    Filter getFilter();
}
