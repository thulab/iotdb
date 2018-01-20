package cn.edu.tsinghua.iotdb.concurrent;

public enum ThreadName {
    JDBC_SERVICE("JDBC-Service"),
    JDBC_CLIENT("JDBC-Client"),
    CLOSE_MERGE_SERVICE("Close-Merge-Service"),
    CLOSE_MERGE_DAEMON("Close-Merge-Daemon-Thread"),
    CLOSE_OPEATION("Close"),
    MERGE_OPERATION("Merge"),
    MEMORY_MONITOR("IoTDB-MemMonitor-thread"),
    MEMORY_STATISTICS("IoTDB-MemStatistic-thread"),
    FLUSH_PARTIAL_POLICY("IoTDB-FlushPartialPolicy-Thread"),
    FORCE_FLUSH_ALL_POLICY("IoTDB-ForceFlushAllPolicy-thread"),
    STAT_MONITOR("StatMonitor-Service"),
    FLUSH_SERVICE("Flush-Service"),
	WAL_FLUSH("WAL-flush");
    
	private String name;
    
    private ThreadName(String name){
    	this.name = name;
    }
    
    public String getName(){
    	return name;
    }
}
