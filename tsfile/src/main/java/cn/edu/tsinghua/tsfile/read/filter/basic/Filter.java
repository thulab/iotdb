package cn.edu.tsinghua.tsfile.read.filter.basic;


import cn.edu.tsinghua.tsfile.read.filter.DigestForFilter;

/**
 * Filter is a top level filter abstraction.
 * Filter has two types of implementations : {@link BinaryFilter} and
 * {@link UnaryFilter}
 *
 */
public interface Filter {

    boolean satisfy(DigestForFilter digest);

    boolean satisfy(long time, Object value);


}
