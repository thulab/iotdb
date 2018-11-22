package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

import java.io.IOException;
import java.util.List;


public interface MetadataQuerier {

    List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException;

    TsFileMetaData getWholeFileMetadata();

}
