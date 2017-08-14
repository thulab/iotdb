package cn.edu.thu.tsfiledb.transferfile.transfer.sender;

import cn.edu.thu.tsfiledb.transferfile.transfer.conf.SenderConfig;

import java.lang.management.ManagementFactory;
import java.util.Timer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lylw on 2017/7/17.
 */
public class Sender implements SenderMBean{
	private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);
	private static SenderConfig config = SenderConfig.getInstance();
	private long timeInterval;
	private long delayTime = 0L;
	private long startTime;
	private Timer timer;
	private static boolean timerTaskRunning = false;

	public static boolean isTimerTaskRunning() {
		return timerTaskRunning;
	}

	public static void setTimerTaskRunning(boolean timerTaskRunning) {
		Sender.timerTaskRunning = timerTaskRunning;
	}

	public long getStartTime() {
		return startTime;
	}

	public void senderService() {
		/** transfer files */
		timer = new Timer();
		timeInterval = config.transferTimeInterval;
		startTime = System.currentTimeMillis() + delayTime;
		timer.schedule(new TransferThread(), delayTime, timeInterval);
	}

	@Override
	public void transferNow() {
		Thread thread = new Thread(new TransferThread());
		thread.start();
	}

	@Override
	public void switchMode(boolean mode) {
		if (mode) {
			/** start schedule */
			timer.cancel();
			timer.purge();
			long nowtime = System.currentTimeMillis();
			while (startTime < nowtime) {
				startTime += timeInterval;
			}
			delayTime = startTime - System.currentTimeMillis();
			timer = new Timer();
			while(delayTime<0)delayTime+=timeInterval;
			timer.schedule(new TransferThread(), delayTime, timeInterval);
		} else {
			timer.cancel();
			timer.purge();
			LOGGER.info("Exit normally");
		}
	}

	@Override
	public void changeTransferTimeInterval(long interval) {
		timeInterval = interval;
		timer.cancel();
		timer.purge();
		timer = new Timer();
		delayTime = startTime - System.currentTimeMillis();
		while(delayTime<0)delayTime+=timeInterval;
		timer.schedule(new TransferThread(), delayTime, timeInterval);
	}
	
	public static void main(String[] args) throws MalformedObjectNameException, InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		Sender transferClientMBean = new Sender();
		transferClientMBean.senderService();
		ObjectName mBeanName = new ObjectName("cn.edu.thu.tsfiledb.transferfile", "type", "Sender");
		mbs.registerMBean(transferClientMBean, mBeanName);
	}

	@Override
	public long getTransferTimeInterval() {
		return timeInterval;
	}
}