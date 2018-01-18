package cn.edu.tsinghua.iotdb.service2;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(RegisterManager.class);
	private List<IService> iServices;
	public RegisterManager(){
		iServices = new ArrayList<>();
	}
	
	public void register(IService service){
		for(IService s: iServices){
			if(s.getID() == service.getID()){
				LOGGER.info("{} has already been registed. skip", service.getID().getName());
				return;
			}
		}
		iServices.add(service);
		service.start();
		LOGGER.info("{} has been registed.", service.getID().getName());
	}
	
	public void deregisterAll(){
		for(IService service: iServices){
			service.stop();
		}
		iServices.clear();
		LOGGER.info("deregister all service.");
	}
}
