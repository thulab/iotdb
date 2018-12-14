package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

public class TestHelper {


	private static String measurementId = "sensor";
	private static String deviceId = "device";

	public static List<ChunkMetaData> createChunkMetaDataList(int count){
		List<ChunkMetaData> ret = new ArrayList<>();
		for(int i = 0;i<count;i++){
			ret.add(cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper.createSimpleTimeSeriesChunkMetaData());
		}
		return ret;
	}

	public static OFSeriesListMetadata createOFSeriesListMetadata(){
		OFSeriesListMetadata ofSeriesListMetadata = new
				OFSeriesListMetadata(measurementId,createChunkMetaDataList(5));
		return ofSeriesListMetadata;
	}

	public static OFRowGroupListMetadata createOFRowGroupListMetadata(){
		OFRowGroupListMetadata ofRowGroupListMetadata = new OFRowGroupListMetadata(deviceId);
		for(int i = 0;i<5;i++){
			ofRowGroupListMetadata.addSeriesListMetaData(createOFSeriesListMetadata());
		}
		return ofRowGroupListMetadata;
	}

	public static OFFileMetadata createOFFileMetadata(){
		OFFileMetadata ofFileMetadata = new OFFileMetadata();
		ofFileMetadata.setLastFooterOffset(100);
		for(int i = 0;i<5;i++){
			ofFileMetadata.addRowGroupListMetaData(createOFRowGroupListMetadata());
		}
		return ofFileMetadata;
	}

  public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
  }
}
