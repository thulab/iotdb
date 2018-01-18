package cn.edu.tsinghua.iotdb.service2;

public interface JDBCServiceMBean {
	String getJDBCServiceStatus();
	int getRPCPort();
    void startService();
    void restartService();
    void stopService();
}
