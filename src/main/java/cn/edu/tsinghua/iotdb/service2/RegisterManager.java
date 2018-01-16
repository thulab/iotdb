package cn.edu.tsinghua.iotdb.service2;

import java.util.ArrayList;
import java.util.List;

public class RegisterManager {
	private List<ISubDaemon> subDaemons;
	public RegisterManager(){
		subDaemons = new ArrayList<>();
	}
	
	public void register(ISubDaemon subDaemon){
		subDaemons.add(subDaemon);
		subDaemon.start();
	}
	
	public void deregisterAll(){
		for(ISubDaemon subDaemon: subDaemons){
			subDaemon.stop();
		}
		subDaemons.clear();
	}
}
