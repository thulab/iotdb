package cn.edu.tsinghua.iotdb.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IoTDBDefaultThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDefaultThreadExceptionHandler.class);
	
	public IoTDBDefaultThreadExceptionHandler(){
	}
	
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		LOGGER.error("Exception in thread {}-{}", t.getName(), t.getId(), e);
	}

	public static void futureTaskHandler(Future<?> future){
		if(future != null){
			try {
				future.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				LOGGER.error("Exception in future task {}", future.toString(), e);
			}
		}
	}
}
