package measurement;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Point {
	private String measurementName;
	private List<String> tagKeys = new LinkedList<String>();
	private List<String> tagValues = new LinkedList<String>();
	private List<String> fieldKeys = new LinkedList<String>();
	private List<Number> fieldValues = new LinkedList<Number>();
	private Date timestamp;
	
	public void appendTag(String tagKey, String tagValue) {
		tagKeys.add(tagKey);
		tagValues.add(tagValue);
	}
	
	public void appendField(String fieldKey, Number fieldValue) {
		fieldKeys.add(fieldKey);
		fieldValues.add(fieldValue);
	}
	
	public void Reset(){
		tagKeys.clear();
		tagValues.clear();
		fieldKeys.clear();
		fieldValues.clear();
	}
	
	public int getFiedNum(){
		return fieldKeys.size();
	}
	
	public String creatInsertStatement(){
		StringBuilder builder = new StringBuilder();
		builder.append("insert into ").append(getPath()).append("(timestamp");
		
		for(String sensor: fieldKeys){
			builder.append(",").append(sensor);
		}
		builder.append(") values(");
		builder.append(timestamp.getTime());
		
		for(Number sensorValue: fieldValues){
			builder.append(",").append(sensorValue);
		}
		builder.append(")");
		return builder.toString();
	}
	
	public String getPath(){
		StringBuilder builder = new StringBuilder();
		builder.append("root.");
		
		int len = tagKeys.size();
		for(int i = 0; i < len; i++){
			builder.append(tagKeys.get(i)).append("--").append(tagValues.get(i)).append(".");
		}
		
		builder.append(measurementName);
		return builder.toString();	
	}
	
	public String getMeasurementName() {
		return measurementName;
	}
	public void setMeasurementName(String measurementName) {
		this.measurementName = measurementName;
	}
	public List<String> getTagKeys() {
		return tagKeys;
	}
	public List<String> getTagValues() {
		return tagValues;
	}
	public List<String> getFieldKeys() {
		return fieldKeys;
	}
	public void setFieldKeys(List<String> fieldKeys) {
		this.fieldKeys = fieldKeys;
	}
	public List<Number> getFieldValues() {
		return fieldValues;
	}
	public void setFieldValues(List<Number> fieldValues) {
		this.fieldValues = fieldValues;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
}
