package cn.edu.tsinghua.iotdb.service;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;

/**
 * This is one server for close and merge regularly
 * 
 * @author liukun
 *
 */
public class CloseMergeService implements IService{

	private static final Logger LOGGER = LoggerFactory.getLogger(CloseMergeService.class);

	private MergeServiceThread mergeService = new MergeServiceThread();
	private CloseServiceThread closeService = new CloseServiceThread();
	private ScheduledExecutorService service;
	private CloseAndMergeDaemon closeAndMergeDaemon = new CloseAndMergeDaemon();

	private static final long mergeDelay = TsfileDBDescriptor.getInstance().getConfig().periodTimeForMerge;
	private static final long closeDelay = TsfileDBDescriptor.getInstance().getConfig().periodTimeForFlush;
	private static final long mergePeriod = TsfileDBDescriptor.getInstance().getConfig().periodTimeForMerge;
	private static final long closePeriod = TsfileDBDescriptor.getInstance().getConfig().periodTimeForFlush;

	private volatile boolean isStart = false;

	private static CloseMergeService CLOSE_MERGE_SERVICE = new CloseMergeService();

	public synchronized static CloseMergeService getInstance() {
		if (CLOSE_MERGE_SERVICE == null) {
			CLOSE_MERGE_SERVICE = new CloseMergeService();
		}
		return CLOSE_MERGE_SERVICE;
	}

	private CloseMergeService() {
		service = IoTDBThreadPoolFactory.newScheduledThreadPool(2, ThreadName.CLOSE_MERGE_SERVICE.getName());
	}

	public void startService() {

		if (!isStart) {
			LOGGER.info("start the close and merge service");
			closeAndMergeDaemon.start();
			isStart = true;
		} else {
			LOGGER.warn("the close and merge service has been already running");
		}
	}

	public void closeService() {

		if (isStart) {
			LOGGER.info("prepare to shutdown the close and merge service");
			isStart = false;
			synchronized (service) {
				service.shutdown();
				service.notify();
			}
			CLOSE_MERGE_SERVICE = null;
			LOGGER.info("shutdown close and merge service successfully");
		} else {
			LOGGER.warn("the close and merge service is not running now");
		}
	}

	private class CloseAndMergeDaemon extends Thread {

		public CloseAndMergeDaemon() {
			super(ThreadName.CLOSE_MERGE_DAEMON.getName());
		}

		@Override
		public void run() {
			service.scheduleWithFixedDelay(mergeService, mergeDelay, mergePeriod, TimeUnit.SECONDS);
			service.scheduleWithFixedDelay(closeService, closeDelay, closePeriod, TimeUnit.SECONDS);
			while (!service.isShutdown()) {
				synchronized (service) {
					try {
						service.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private class MergeServiceThread extends Thread {

		public MergeServiceThread() {
			super(ThreadName.MERGE_OPERATION.getName());
		}

		@Override
		public void run() {
			LOGGER.info("start the merge action regularly");
			try {
				FileNodeManager.getInstance().mergeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				LOGGER.error("merge all error, the reason is {}", e.getMessage());
			}
		}
	}

	private class CloseServiceThread extends Thread {

		public CloseServiceThread() {
			super(ThreadName.CLOSE_OPEATION.getName());
		}

		@Override
		public void run() {
			LOGGER.info("start the close action regularly");
			try {
				FileNodeManager.getInstance().closeAll();
			} catch (FileNodeManagerException e) {
				e.printStackTrace();
				LOGGER.error("close all error, the reason is {}", e.getMessage());
			}
		}
	}

	@Override
	public void start() {
		startService();
	}

	@Override
	public void stop() {
		closeService();
	}

	@Override
	public ServiceType getID() {
		return ServiceType.CLOSE_MERGE_SERVICE;
	}
}
