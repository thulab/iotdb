package devSimulator;

import java.util.ArrayList;
import java.util.List;

public class Region {
	private String name;
	List<String> dataCenters;
	public Region(String name, String[] dataCenters){
		this.name = name;
		this.dataCenters = java.util.Arrays.asList(dataCenters);
	}
	
	public void appendDataCenter(String dataCenter){
		dataCenters.add(dataCenter);
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getDataCenters() {
		return dataCenters;
	}
	public void setDataCenters(List<String> dataCenters) {
		this.dataCenters = dataCenters;
	}
}
