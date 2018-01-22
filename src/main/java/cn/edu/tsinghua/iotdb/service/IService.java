package cn.edu.tsinghua.iotdb.service;

public interface IService {
	/**
	 * Start current service.
	 */
	void start();

	/**
	 * Stop current service.
	 * If current service uses thread or thread pool,
	 * current service should guarantee to release thread or thread pool.
	 */
	void stop();

	/**
	 * @return current service name
	 */
	ServiceType getID();
}
