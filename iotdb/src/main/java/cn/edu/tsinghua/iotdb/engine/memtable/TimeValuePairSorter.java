package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;

import java.util.List;


public interface TimeValuePairSorter {

    /**
     * @return a List which contains all distinct {@link TimeValuePair}s in ascending order by timestamp.
     */
    List<TimeValuePair> getSortedTimeValuePairList();
}
