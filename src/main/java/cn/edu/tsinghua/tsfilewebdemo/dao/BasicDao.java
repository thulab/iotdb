package cn.edu.tsinghua.tsfilewebdemo.dao;

import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfilewebdemo.bean.TimeValues;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
public interface BasicDao {

    List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange);

    List<String> getMetaData();

}
