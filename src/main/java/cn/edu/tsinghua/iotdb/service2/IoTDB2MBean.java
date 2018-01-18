package cn.edu.tsinghua.iotdb.service2;

import java.io.IOException;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;

public interface IoTDB2MBean {
	void stop() throws FileNodeManagerException, IOException;
}
