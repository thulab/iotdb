package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.read.datatype.TimeValuePair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MemSeriesLazyMerger implements TimeValuePairSorter{

    private List<IMemSeries> memSeriesList;

    public MemSeriesLazyMerger() {
        memSeriesList = new ArrayList<>();
    }

    public MemSeriesLazyMerger(IMemSeries... memSerieses) {
        this();
        Collections.addAll(memSeriesList, memSerieses);
    }

    /**
     * IMPORTANT: Please ensure that the minimum timestamp of added {@link IMemSeries} is larger than
     * any timestamps of the IMemSeries already added in.
     * @param series
     */
    public void addMemSeries(IMemSeries series) {
        memSeriesList.add(series);
    }

    public List<TimeValuePair> getSortedTimeValuePairList() {
        if (memSeriesList.size() == 0) {
            return new ArrayList<>();
        } else {
            List<TimeValuePair> ret = memSeriesList.get(0).getSortedTimeValuePairList();
            for (int i = 1; i < memSeriesList.size(); i++) {
                ret.addAll(memSeriesList.get(i).getSortedTimeValuePairList());
            }
            return ret;
        }
    }
}
