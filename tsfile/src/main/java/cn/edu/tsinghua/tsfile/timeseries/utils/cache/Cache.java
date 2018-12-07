package cn.edu.tsinghua.tsfile.timeseries.utils.cache;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;

import java.io.IOException;


public interface Cache<K, T> {
    T get(K key) throws CacheException, IOException;

    void clear();
}
