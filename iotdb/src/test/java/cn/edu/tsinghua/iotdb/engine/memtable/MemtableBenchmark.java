package cn.edu.tsinghua.iotdb.engine.memtable;


import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

public class MemtableBenchmark {

    private static String deviceId = "d0";
    private static int numOfMeasurement = 1000;
    private static String[] measurementId  = new String[numOfMeasurement];

    static {
        for(int i = 0;i< numOfMeasurement;i++){
            measurementId[i] = String.valueOf("m"+i);
        }
    }

    private static TSDataType tsDataType = TSDataType.INT64;
    private static int numOfPoint = 10000;

    public static void main(String[] args) {
        IMemTable memTable = new PrimitiveMemTable();
        final long startTime = System.currentTimeMillis();
        for(int i = 0;i<numOfPoint;i++){
            for(int j = 0;j<numOfMeasurement;j++){
                memTable.write(deviceId,measurementId[j],tsDataType,System.nanoTime(),String.valueOf(System.currentTimeMillis()));
            }
        }
        final long endTime = System.currentTimeMillis();
        System.out.println(String.format("Num of time series: %d, " +
                "Num of points for each time series: %d, " +
                "The total time: %d ms. ",numOfMeasurement,numOfPoint,endTime-startTime));
    }
}
