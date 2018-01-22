package cn.edu.tsinghua.iotdb.service;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.iotdb.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.iotdb.conf.TsFileConstant;

public class StartupChecks {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartupChecks.class);
    
    private final List<StartupCheck> preChecks = new ArrayList<>();
    private final List<StartupCheck> DEFALUT_TESTS = new ArrayList<>();
    
    public StartupChecks(){
	DEFALUT_TESTS.add(checkJMXPort);
    }
    
    public StartupChecks withDefaultTest(){
	preChecks.addAll(DEFALUT_TESTS);
	return this;
    }
    
    public StartupChecks withMoreTest(StartupCheck check){
	preChecks.add(check);
	return this;
    }
    
    public void verify() throws StartupException {
	for(StartupCheck check : preChecks){
	    check.execute();
	}
    }
    
    public static final StartupCheck checkJMXPort = new StartupCheck() {

        @Override
        public void execute() throws StartupException {
    		String jmxPort = System.getProperty(TsFileConstant.REMOTE_JMX_PORT_NAME);
    		if(jmxPort == null){
    		    LOGGER.warn("JMX is not enabled to receive remote connection. "
    		    	+ "Please check conf/{}.sh(Unix or OS X, if you use Windows, check conf/{}.bat) for more info", 
    		    	TsFileConstant.ENV_FILE_NAME,TsFileConstant.ENV_FILE_NAME);
    		    jmxPort = System.getProperty(TsFileConstant.TSFILEDB_LOCAL_JMX_PORT_NAME);
    		    if(jmxPort == null){
    			LOGGER.warn("{} missing from {}.sh(Unix or OS X, if you use Windows, check conf/{}.bat)", 
    					TsFileConstant.TSFILEDB_LOCAL_JMX_PORT_NAME, TsFileConstant.ENV_FILE_NAME, TsFileConstant.ENV_FILE_NAME);
    		    }
    		}else{
    		    LOGGER.info("JMX is enabled to receive remote connection on port {}", jmxPort);
    		}
        }
    };
}
