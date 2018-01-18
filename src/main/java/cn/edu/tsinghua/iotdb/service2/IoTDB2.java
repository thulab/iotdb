package cn.edu.tsinghua.iotdb.service2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.IoTDBConstant;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.service.StartupChecks;

public class IoTDB2 implements IoTDB2MBean{
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB2.class);
	private RegisterManager registerManager = new RegisterManager();
    private final String MBEAN_NAME = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, "IoTDB");
	
    private static class IoTDB2Holder {
		private static final IoTDB2 INSTANCE = new IoTDB2();
	}

	public static final IoTDB2 getInstance() {
		return IoTDB2Holder.INSTANCE;
	}
	
	public void active() {
		StartupChecks checks = new StartupChecks().withDefaultTest();
		try {
			checks.verify();
		} catch (StartupException e) {
			LOGGER.error("{}: failed to start because some checks failed. {}", IoTDBConstant.GLOBAL_DB_NAME, e.getMessage());
			return;
		}
		setUp();
//		try {
//			setUp();
//		} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | TTransportException | IOException e) {
//			LOGGER.error("{}: failed to start because: {}", TsFileDBConstant.GLOBAL_DB_NAME, e.getMessage());
//		} catch (FileNodeManagerException e) {
//			e.printStackTrace();
//		} catch (PathErrorException e) {
//			e.printStackTrace();
//		}
	}
	
	
	private void setUp(){
		setUncaughtExceptionHandler();
		
		// TODO do some recovery
		registerManager.register(JMXService.getInstance());
		registerManager.register(JDBCService.getInstance());

		JMXService.registerMBean(getInstance(), MBEAN_NAME);
	}
	
	public void deactivate(){
		registerManager.deregisterAll();
		JMXService.deregisterMBean(MBEAN_NAME);
	}
	
	@Override
	public void stop() {
		deactivate();
	}
	
	private void setUncaughtExceptionHandler(){
		Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
	            public void uncaughtException(Thread t, Throwable e) {
	            	LOGGER.error("Exception in thread {}-{}", t.getName(), t.getId(), e);
	            }
		});
	}
	
	public static void main(String[] args) {
		IoTDB2 daemon = IoTDB2.getInstance();
		daemon.active();

	}

}
