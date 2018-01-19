package cn.edu.tsinghua.iotdb.service;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.auth.dao.DBDaoService;
import cn.edu.tsinghua.iotdb.conf.IoTDBConstant;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.memcontrol.BasicMemController;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.exception.PathErrorException;
import cn.edu.tsinghua.iotdb.exception.StartupException;
import cn.edu.tsinghua.iotdb.monitor.StatMonitor;
import cn.edu.tsinghua.iotdb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.DeletePlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.InsertPlan;
import cn.edu.tsinghua.iotdb.qp.physical.crud.UpdatePlan;
import cn.edu.tsinghua.iotdb.sys.writelog.WriteLogManager;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class IoTDB implements IoTDBMBean{
	private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
	private RegisterManager registerManager = new RegisterManager();
    private final String MBEAN_NAME = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE, "IoTDB");
	
    private static class IoTDB2Holder {
		private static final IoTDB INSTANCE = new IoTDB();
	}

	public static final IoTDB getInstance() {
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
	}
	
	
	private void setUp() {
		setUncaughtExceptionHandler();
		
		FileNodeManager.getInstance().recovery();
		try {
			systemDataRecovery();
		} catch (PathErrorException | IOException | FileNodeManagerException e) {
			LOGGER.error("{}: failed to start because: {}", IoTDBConstant.GLOBAL_DB_NAME, e.getMessage());
			return;
		}
		if (TsfileDBDescriptor.getInstance().getConfig().enableStatMonitor){
			StatMonitor.getInstance().recovery();
		}

		registerManager.register(FileNodeManager.getInstance());
		registerManager.register(WriteLogManager.getInstance());
		IService DBDaoService = new DBDaoService();
		registerManager.register(DBDaoService);
		registerManager.register(JMXService.getInstance());
		registerManager.register(JDBCService.getInstance());
		registerManager.register(Monitor.INSTANCE);
		registerManager.register(CloseMergeService.getInstance());
		registerManager.register(StatMonitor.getInstance());
		registerManager.register(BasicMemController.getInstance());
		
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
	
	/**
	 * Recover data using system log.
	 *
	 * @throws IOException
	 */
	private void systemDataRecovery() throws IOException, FileNodeManagerException, PathErrorException {
		LOGGER.info("{}: start checking write log...", IoTDBConstant.GLOBAL_DB_NAME);
		// QueryProcessor processor = new QueryProcessor(new OverflowQPExecutor());
		WriteLogManager writeLogManager = WriteLogManager.getInstance();
		writeLogManager.recovery();
		long cnt = 0L;
		PhysicalPlan plan;
		WriteLogManager.isRecovering = true;
		while ((plan = writeLogManager.getPhysicalPlan()) != null) {
			try {
				if (plan instanceof InsertPlan) {
					InsertPlan insertPlan = (InsertPlan) plan;
					WriteLogRecovery.multiInsert(insertPlan);
				} else if (plan instanceof UpdatePlan) {
					UpdatePlan updatePlan = (UpdatePlan) plan;
					WriteLogRecovery.update(updatePlan);
				} else if (plan instanceof DeletePlan) {
					DeletePlan deletePlan = (DeletePlan) plan;
					WriteLogRecovery.delete(deletePlan);
				}
				cnt++;
			} catch (ProcessorException e) {
				e.printStackTrace();
				throw new IOException("Error in recovery from write log");
			}
		}
		WriteLogManager.isRecovering = false;
		LOGGER.info("{}: Done. Recover operation count {}", IoTDBConstant.GLOBAL_DB_NAME, cnt);
	}
	
	
	public static void main(String[] args) {
		IoTDB daemon = IoTDB.getInstance();
		daemon.active();

	}

}
