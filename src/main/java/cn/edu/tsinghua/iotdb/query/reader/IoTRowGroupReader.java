package cn.edu.tsinghua.iotdb.query.reader;

import cn.edu.tsinghua.iotdb.engine.tombstone.Tombstone;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.RowGroupReader;
import cn.edu.tsinghua.tsfile.timeseries.read.ValueReader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IoTRowGroupReader extends RowGroupReader {

    private List<Tombstone> tombstones;
    private long writtenTime;

    public IoTRowGroupReader(RowGroupMetaData rowGroupMetaData, ITsRandomAccessFileReader raf, List<Tombstone> tombstoneList) {
        seriesDataTypeMap = new HashMap<>();
        deltaObjectUID = rowGroupMetaData.getDeltaObjectID();
        measurementIds = new ArrayList<>();
        this.totalByteSize = rowGroupMetaData.getTotalByteSize();
        this.raf = raf;
        this.writtenTime = rowGroupMetaData.getWrittenTime();
        this.tombstones = tombstoneList;

        initValueReaders(rowGroupMetaData);
    }

    @Override
    public void initValueReaders(RowGroupMetaData rowGroupMetaData) {
        for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
            if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
                String measurementId = tscMetaData.getProperties().getMeasurementUID();
                measurementIds.add(measurementId);
                seriesDataTypeMap.put(measurementId,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());

                // get the max tombstone of this series
                long maxTombstoneTime = 0;
                for(Tombstone tombstone : tombstones) {
                    if(measurementId.equals(tombstone.measurementId) && writtenTime < tombstone.executeTimestamp)
                        maxTombstoneTime = tombstone.deleteTimestamp > maxTombstoneTime ? tombstone.deleteTimestamp : maxTombstoneTime;
                }

                ValueReader si = new IoTValueReader(tscMetaData.getProperties().getFileOffset(),
                        tscMetaData.getTotalByteSize(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows(),
                        tscMetaData.getTInTimeSeriesChunkMetaData().getStartTime(), tscMetaData.getTInTimeSeriesChunkMetaData().getEndTime(),
                        maxTombstoneTime, deltaObjectUID, measurementId);
                valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
            }
        }
    }
}
