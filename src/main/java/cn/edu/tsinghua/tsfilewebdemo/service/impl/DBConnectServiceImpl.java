package cn.edu.tsinghua.tsfilewebdemo.service.impl;

import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfilewebdemo.bean.TimeValues;
import cn.edu.tsinghua.tsfilewebdemo.dao.BasicDao;
import cn.edu.tsinghua.tsfilewebdemo.service.DBConnectService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by dell on 2017/7/17.
 */
@Service
public class DBConnectServiceImpl implements DBConnectService {

    @Autowired
    BasicDao basicDao;

    @Override
    public int testConnection() {
        return 0;
    }

    @Override
    public List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange) {
        return basicDao.querySeries(s, timeRange);
    }

    @Override
    public List<String> getMetaData() {
        return basicDao.getMetaData();
    }

}
