package cn.edu.tsinghua.iotdb.concurrent;

public enum ThreadName {
    JDBC_SERVICE(""),
    JDBC_CLIENT("");
    
	private String name;
    
    public ThreadName(String name){
    	this.name = name;
    }
    
    
}
