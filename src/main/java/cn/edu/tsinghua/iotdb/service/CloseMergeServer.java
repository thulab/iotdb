package cn.edu.tsinghua.iotdb.service;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;

/**
 * This is one server for close and merge regularly
 * 
 * @author liukun
 *
 */
public class CloseMergeServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(CloseMergeServer.class);

	private MergeServerThread mergeServer = new MergeServerThread();
	private CloseServerThread closeServer = new CloseServerThread();
	private ScheduledThreadPoolExecutor service;
	private CloseAndMergeDaemon closeAndMergeDaemon = new CloseAndMergeDaemon();

	private static final long mergeDelay = TsfileDBDescriptor.getInstance().getConfig().periodTimeForMerge;
	private static final long closeDelay = TsfileDBDescriptor.getInstance().getConfig().periodTimeForFlush;
	private static final long mergePeriod = TsfileDBDescriptor.getInstance().getConfig().periodTimeForMerge;
	private static final long closePeriod = TsfileDBDescriptor.getInstance().getConfig().periodTimeForFlush;

	private volatile boolean isStart = false;

	private static CloseMergeServer SERVER = new CloseMergeServer();

	public synchronized static CloseMergeServer getInstance() {
		if (SERVER == null) {
			SERVER = new CloseMergeServer();
		}
		return SERVER;
	}

	private CloseMergeServer() {
		service = new ScheduledThreadPoolExecutor(2);
	}

	public void startServer() {

		if (!isStart) {
			LOGGER.info("start the close and merge server");
			closeAndMergeDaemon.start();
			isStart = true;
		} else {
			LOGGER.warn("the close and merge daemon has been already running");
		}
	}

	public void closeServer() {

		if (isStart) {
			LOGGER.info("shutdown the close and merge server");
			isStart = false;
			synchronized (service) {
				service.shutdown();
				service.notify();
			}
			SERVER = null;
		} else {
			LOGGER.warn("the close and merge daemon is not running now");
		}
	}

	private class CloseAndMergeDaemon extends Thread {

		public CloseAndMergeDaemon() {
			super("MergeAndCloseServer");
		}

		@Override
		public void run() {
			service.scheduleWithFixedDelay(mergeServer, mergeDelay, mergePeriod, TimeUnit.SECONDS);
			service.scheduleWithFixedDelay(closeServer, closeDelay, closePeriod, TimeUnit.SECONDS);
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

	private class MergeServerThread extends Thread {

		public MergeServerThread() {
			super("merge_server_thread");
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

	private class CloseServerThread extends Thread {

		public CloseServerThread() {
			super("close_server_thread");
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
}
