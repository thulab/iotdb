package cn.edu.thu.tsfiledb.service;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.engine.exception.FileNodeManagerException;
import cn.edu.thu.tsfiledb.engine.filenode.FileNodeManager;

public class MergeServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MergeServer.class);
	
	private MergeServerThread mergeServer = new MergeServerThread();
	private CloseServerThre closeServer = new CloseServerThre();
	private ScheduledThreadPoolExecutor service;
	private CloseAndMergeDaemon Daemon = new CloseAndMergeDaemon();
	
	private static final long mergeDelay = 12;
	private static final long closeDelay = 1;
	private static final long mergePeriod = 12;
	private static final long closePeriod = 1;
	
	private boolean isStart = false;
	
	private static final MergeServer SERVER = new MergeServer();
	
	public static MergeServer getInstance(){
		return SERVER;
	}

	private MergeServer() {
		service = new ScheduledThreadPoolExecutor(2);
	}
	
	public void startServer() {
		
		if (!isStart) {
			LOGGER.info("start the close and merge daemon");
			Daemon.start();
			isStart = true;
		}else{
			LOGGER.info("the close and merge daemon has been already running");
		}
		
	}

	public void closeServer() {
		if(isStart){
			LOGGER.info("shutdown the close and merge daemon");
			service.shutdown();
			isStart = false;
		}else{
			LOGGER.info("the close and merge daemon is not running now");
		}
	}

	private class CloseAndMergeDaemon extends Thread {

		public CloseAndMergeDaemon() {
			super("MergeAndCloseServer");
			this.setDaemon(true);
		}

		@Override
		public void run() {
			service.scheduleWithFixedDelay(mergeServer, mergeDelay, mergePeriod, TimeUnit.HOURS);
			service.scheduleWithFixedDelay(closeServer, closeDelay, closePeriod, TimeUnit.HOURS);
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
				// System.exit(0);
			}
		}
	}

	private class CloseServerThre extends Thread {

		public CloseServerThre() {
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
				// System.exit(0);
			}
		}
	}

}
