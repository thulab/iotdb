package cn.edu.tsinghua.tsfile.timeseries.utils.cache;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public interface Cache<K, T> {
    T get(K key) throws CacheException, IOException;

    void clear();
}
