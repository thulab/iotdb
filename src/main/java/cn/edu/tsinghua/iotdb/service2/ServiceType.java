package cn.edu.tsinghua.iotdb.service2;

public enum ServiceType {
	DBDAO_SERVICE("",""),
	FILE_NODE_SERVICE("",""),
	JMX_SERVICE("JMX Service","JMX Service"),
	JDBC_SERVICE("JDBC Service","JDBCService"),
	STAT_MONITOR_SERVICE("",""),
	WAL_SERVICE("",""),
	CLOSE_MERGE_SERVICE("",""),
	JVM_MEM_CONTROL_SERVICE("","");
	
	private String name;  
	private String jmxName;

	private ServiceType(String name, String jmxName) {  
        this.name = name;  
        this.jmxName = jmxName;
    }  

    public String getName() {  
        return name;  
    }  

    public String getJmxName() {
    	return jmxName;
    }
}
