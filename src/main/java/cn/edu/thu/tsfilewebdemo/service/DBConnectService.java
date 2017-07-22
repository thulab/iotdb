package cn.edu.thu.tsfilewebdemo.service;

import cn.edu.thu.tsfilewebdemo.bean.TimeValues;
import javafx.util.Pair;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
public interface DBConnectService {

    int testConnection();

    List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange);

    List<String> getMetaData();

}
