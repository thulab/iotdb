package cn.edu.tsinghua.postback.iotdb.receiver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkProperties;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObject;
import cn.edu.tsinghua.tsfile.file.metadata.TsRowGroupBlockMetaData;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.FileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;

public class Test {

	public static void main(String[] args) throws IOException {

		Test test = new Test();
		test.a();
	}
	
	void a() throws IOException {
		/**
		 * 打开TsFile文件进行解析
		 */
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();
		String path = "C:\\Users\\lta\\Desktop\\1517484405417-1517486202972";
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);           
        FileReader reader = new FileReader(input);            
        Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
        Iterator<String> it = deltaObjectMap.keySet().iterator();
        while(it.hasNext()) {
        	String key = it.next().toString(); //key represent storage group
//        	System.out.println("key:" + key);
        	TsDeltaObject deltaObj = deltaObjectMap.get(key);
//        	System.out.println(deltaObj.startTime);
        	if(deltaObj.startTime == -1) {
        		System.out.println(deltaObj);
        	}
        	TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
       // 	System.out.println("blockMeta ID:" + blockMeta.getDeltaObjectID());
            blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input, deltaObj.offset, deltaObj.metadataBlockSize));
            List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
//            System.out.println(rowGroupMetadataList.size());
//            if(rowGroupMetadataList.size()!=1) {
//            	System.out.println("sleep");
//            }
            for(RowGroupMetaData rowGroupMetaData:rowGroupMetadataList)
            {
            	long startTime = 0x7fffffffffffL;
            	long endTime = 0;
//            	System.out.println("rowGroupMetaData ID:" + rowGroupMetaData.getDeltaObjectID());
            	List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
            	for(TimeSeriesChunkMetaData timeSeriesChunkMetaData:timeSeriesChunkMetaDataList)
            	{
            		TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData.getTInTimeSeriesChunkMetaData();
            		TimeSeriesChunkProperties properties =timeSeriesChunkMetaData.getProperties();
            		String measurementUID = properties.getMeasurementUID();
            		if(startTime==-1)
            		{
	            		System.out.println(rowGroupMetaData.getDeltaObjectID() + measurementUID);	
	            		System.out.println(measurementUID);	
            		}
            		startTime = Math.min(tInTimeSeriesChunkMetaData.getStartTime(),startTime);
            		endTime = Math.max(tInTimeSeriesChunkMetaData.getEndTime(), endTime);
            	}
            	if(startTime==-1) {
	            	System.out.println(rowGroupMetaData.getDeltaObjectID());
	            	System.out.println(startTime);
	            	System.out.println(endTime);
	            	return;
	            }
            	startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), startTime);
            	endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), endTime);
            }
        }  
        System.out.println(startTimeMap);
        System.out.println(endTimeMap);
	}
	void b(){
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();
		TsRandomAccessLocalFileReader input = null;
		try {
			input = new TsRandomAccessLocalFileReader("C:\\Users\\lta\\Desktop\\1517459310667-1517463136376");
			FileReader reader = new FileReader(input);
			Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
			Iterator<String> it = deltaObjectMap.keySet().iterator();
			while (it.hasNext()) {
				String key = it.next().toString(); // key represent storage group
				TsDeltaObject deltaObj = deltaObjectMap.get(key);
				TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
				blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
						deltaObj.offset, deltaObj.metadataBlockSize));
				List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
				for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
					long startTime = 0x7fffffffffffffffL;
					long endTime = 0;
					List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
							.getTimeSeriesChunkMetaDataList();
					for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
						TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
								.getTInTimeSeriesChunkMetaData();
						TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
						String measurementUID = properties.getMeasurementUID();
						measurementUID = key + "." + measurementUID;
						startTime = Math.min(tInTimeSeriesChunkMetaData.getStartTime(), startTime);
						endTime = Math.max(tInTimeSeriesChunkMetaData.getEndTime(), endTime);
					}
					if(startTime==-1) {
		            	System.out.println(rowGroupMetaData.getDeltaObjectID());
		            	return;}
					System.out.println(startTime);
					startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), startTime);
					endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), endTime);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				input.close();
			} catch (IOException e) {
			}
		}
	}
	void c() {
		Map<String, Long> startTimeMap = new HashMap<>();
		Map<String, Long> endTimeMap = new HashMap<>();
		TsRandomAccessLocalFileReader input = null;
		try {
			input = new TsRandomAccessLocalFileReader("C:\\Users\\lta\\Desktop\\1517459310667-1517463136376");
			FileReader reader = new FileReader(input);
			Map<String, TsDeltaObject> deltaObjectMap = reader.getFileMetaData().getDeltaObjectMap();
			Iterator<String> it = deltaObjectMap.keySet().iterator();
			while (it.hasNext()) {
				String key = it.next().toString(); // key represent storage group
				TsDeltaObject deltaObj = deltaObjectMap.get(key);
				TsRowGroupBlockMetaData blockMeta = new TsRowGroupBlockMetaData();
				blockMeta.convertToTSF(ReadWriteThriftFormatUtils.readRowGroupBlockMetaData(input,
						deltaObj.offset, deltaObj.metadataBlockSize));
				List<RowGroupMetaData> rowGroupMetadataList = blockMeta.getRowGroups();
				for (RowGroupMetaData rowGroupMetaData : rowGroupMetadataList) {
					long startTime = 0x7fffffffffffffffL;
					long endTime = 0;
					List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList = rowGroupMetaData
							.getTimeSeriesChunkMetaDataList();
					for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataList) {
						TInTimeSeriesChunkMetaData tInTimeSeriesChunkMetaData = timeSeriesChunkMetaData
								.getTInTimeSeriesChunkMetaData();
						TimeSeriesChunkProperties properties = timeSeriesChunkMetaData.getProperties();
						String measurementUID = properties.getMeasurementUID();
						measurementUID = key + "." + measurementUID;
						startTime = Math.min(tInTimeSeriesChunkMetaData.getStartTime(), startTime);
						endTime = Math.max(tInTimeSeriesChunkMetaData.getEndTime(), endTime);
					}
					if(startTime==-1) {
		            	System.out.println(rowGroupMetaData.getDeltaObjectID());
		            	return;}
					System.out.println(startTime);
					startTimeMap.put(rowGroupMetaData.getDeltaObjectID(), startTime);
					endTimeMap.put(rowGroupMetaData.getDeltaObjectID(), endTime);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				input.close();
			} catch (IOException e) {
			}
		}
	}
}
