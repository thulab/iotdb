package cn.edu.tsinghua.iotdb.engine.tombstone;


import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class TombstoneMergeTask implements Callable<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TombstoneMergeTask.class);
    private List<TombstoneMerger> mergerList;
    private FileNodeProcessor processor;

    public TombstoneMergeTask(List<TombstoneMerger> mergerList, FileNodeProcessor processor) {
        this.mergerList = mergerList;
        this.processor = processor;
    }

    @Override
    public Void call() throws Exception {
        for(TombstoneMerger merger : mergerList)
            merger.merge();
        processor.close();
        LOGGER.info("Thread {} completes {} tombstone merge tasks.", Thread.currentThread().getName(), mergerList.size());
        return null;
    }
}
