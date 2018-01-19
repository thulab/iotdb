package cn.edu.tsinghua.iotdb.service;

public interface IService {
	void start();
	void stop();
	ServiceType getID();
}
