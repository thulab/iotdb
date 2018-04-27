package cn.edu.tsinghua.iotdb.engine.tombstone;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class TombstoneMergeTask implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneMergeTask.class);
    private List<TombstoneMerger> mergerList;

    public TombstoneMergeTask(List<TombstoneMerger> mergerList) {
        this.mergerList = mergerList;
    }

    @Override
    public Void call() throws Exception {
        for(TombstoneMerger merger : mergerList)
            merger.merge();
        LOGGER.info("Thread {} completes {} tombstone merge tasks.", Thread.currentThread().getName(), mergerList.size());
        return null;
    }
}
