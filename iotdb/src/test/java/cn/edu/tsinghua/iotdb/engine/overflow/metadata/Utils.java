package cn.edu.tsinghua.iotdb.engine.overflow.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.util.List;

import static org.junit.Assert.*;

public class Utils {

	public static void isOFSeriesListMetadataEqual(OFSeriesListMetadata ofSeriesListMetadata1, OFSeriesListMetadata ofSeriesListMetadata2){
		if(cn.edu.tsinghua.tsfile.file.metadata.utils.Utils.isTwoObjectsNotNULL(ofSeriesListMetadata1,ofSeriesListMetadata2,"OFSeriesListMetadata")){
			if(cn.edu.tsinghua.tsfile.file.metadata.utils.Utils.
					isTwoObjectsNotNULL(ofSeriesListMetadata1.getMeasurementId(),
							ofSeriesListMetadata2.getMeasurementId(),"measurement id")){
				assertTrue(ofSeriesListMetadata1.getMeasurementId().equals(ofSeriesListMetadata2.getMeasurementId()));
			}
			assertEquals(ofSeriesListMetadata1.getMetaDatas().size(),ofSeriesListMetadata2.getMetaDatas().size());
			List<ChunkMetaData> chunkMetaDataList1 = ofSeriesListMetadata1.getMetaDatas();
			List<ChunkMetaData> chunkMetaDataList2 = ofSeriesListMetadata2.getMetaDatas();
			for(int i = 0;i<chunkMetaDataList1.size();i++){
				cn.edu.tsinghua.tsfile.file.metadata.utils.Utils.
						isTimeSeriesChunkMetadataEqual(chunkMetaDataList1.get(i),chunkMetaDataList2.get(i));
			}
		}
	}

	public static void isOFRowGroupListMetadataEqual(OFRowGroupListMetadata ofRowGroupListMetadata1, OFRowGroupListMetadata ofRowGroupListMetadata2){
		if(cn.edu.tsinghua.tsfile.file.metadata.utils.Utils.
				isTwoObjectsNotNULL(ofRowGroupListMetadata1,
						ofRowGroupListMetadata2,"OFRowGroupListMetadata")){
			assertTrue(ofRowGroupListMetadata1.getDeltaObjectId().equals(ofRowGroupListMetadata2.getDeltaObjectId()));
			List<OFSeriesListMetadata> list1 = ofRowGroupListMetadata1.getSeriesList();
			List<OFSeriesListMetadata> list2 = ofRowGroupListMetadata2.getSeriesList();
			assertEquals(list1.size(),list2.size());
			for(int i = 0;i<list1.size();i++){
				isOFSeriesListMetadataEqual(list1.get(i),list2.get(i));
			}
		}
	}

	public static void isOFFileMetadataEqual(OFFileMetadata ofFileMetadata1, OFFileMetadata ofFileMetadata2){
		if(cn.edu.tsinghua.tsfile.file.metadata.utils.Utils.isTwoObjectsNotNULL(ofFileMetadata1,ofFileMetadata2,"OFFileMetadata")){
			assertEquals(ofFileMetadata1.getLastFooterOffset(),ofFileMetadata2.getLastFooterOffset());
			List<OFRowGroupListMetadata> list1 = ofFileMetadata1.getRowGroupLists();
			List<OFRowGroupListMetadata> list2 = ofFileMetadata2.getRowGroupLists();
			assertEquals(list1.size(),list2.size());
			for(int i = 0;i<list1.size();i++){
				isOFRowGroupListMetadataEqual(list1.get(i),list2.get(i));
			}
		}
	}

}
