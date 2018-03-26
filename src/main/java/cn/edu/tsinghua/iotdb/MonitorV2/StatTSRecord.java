package cn.edu.tsinghua.iotdb.MonitorV2;

import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StatTSRecord extends TSRecord {

    public StatTSRecord(long timestamp, String path) {
        super(timestamp, path);
        for(MonitorConstants.StatisticConstants type : MonitorConstants.StatisticConstants.values()){
            addTuple(new LongDataPoint(type.name(), 0));
        }
    }

    public StatTSRecord(StatTSRecord record, String path){
        super(record.time, path);
        for(DataPoint dataPoint : record.dataPointList){
            addTuple(new LongDataPoint(dataPoint.getMeasurementId(), (long)dataPoint.getValue()));
        }
    }

    public StatTSRecord(long timestamp, String path, Map<String, Long> stats){
        this(timestamp, path);
        for(Map.Entry<String, Long> stat : stats.entrySet()){
            addOneStatistic(MonitorConstants.StatisticConstants.valueOf(stat.getKey()), stat.getValue());
        }
    }

    public void addOneStatistic(MonitorConstants.StatisticConstants type, StatTSRecord record){
        for(int i = 0;i < dataPointList.size();i++){
            if(dataPointList.get(i).getMeasurementId().equals(type.name())){
                long old_value = (long)dataPointList.get(i).getValue();
                dataPointList.get(i).setLong(old_value + (long)record.getConstantValue(type));
            }
        }
    }

    public void addOneStatistic(MonitorConstants.StatisticConstants type, long value){
        for(int i = 0;i < dataPointList.size();i++){
            if(dataPointList.get(i).getMeasurementId().equals(type.name())){
                long old_value = (long)dataPointList.get(i).getValue();
                dataPointList.get(i).setLong(old_value + value);
            }
        }
    }

    public static List<String> getAllPaths(String prefix){
        List<String> paths = new ArrayList<>();
        for(MonitorConstants.StatisticConstants statistic : MonitorConstants.StatisticConstants.values()){
            paths.add(prefix + MonitorConstants.STATISTIC_PATH_SEPERATOR + statistic.name());
        }
        return paths;
    }

    public int getConstantIndex(MonitorConstants.StatisticConstants constants){
        for(int i = 0;i < dataPointList.size();i++){
            if(dataPointList.get(i).getMeasurementId().equals(constants.name()))return i;
        }
        return -1;
    }

    public Object getConstantValue(MonitorConstants.StatisticConstants constants){
        int index = getConstantIndex(constants);
        if(index == -1)return null;
        else return dataPointList.get(index).getValue();
    }
}
