package cn.edu.tsinghua.iotdb.service;

public interface JDBCServiceMBean {
    String getJDBCServiceStatus();
    int getRPCPort();
    void startService();
    void restartService();
    void stopService();
}
