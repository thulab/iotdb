package cn.edu.tsinghua.iotdb.service2;

public interface ISubDaemon {
	void start();
	void stop();
	String getID();
}
