package cn.edu.tsinghua.tsfile.timeseries.filter.basic;


import cn.edu.tsinghua.tsfile.timeseries.filter.DigestForFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

/**
 * Filter is a top level filter abstraction.
 * Filter has two types of implementations : {@link BinaryFilter} and
 * {@link UnaryFilter}
 * Filter is a role of interviewee in visitor pattern.
 *
 */
public interface Filter {

    boolean satisfy(DigestForFilter digest);

    boolean satisfy(TimeValuePair pair);

    boolean satisfy(long time, Object value);

}
