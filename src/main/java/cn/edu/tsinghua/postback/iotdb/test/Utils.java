package cn.edu.tsinghua.postback.iotdb.test;
/**
 * Created by stefanie on 07/08/2017.
 */

public class Utils {

//    private static String[] typeAndEncode = new String[2];
//    private static String[] pathAndSensor = new String[2];

//    private static void splitPathAndSensor(String timeseries) {
//
//        int lastPointIndex = timeseries.indexOf("s");
//        pathAndSensor[0] = timeseries.substring(0, lastPointIndex - 1);
//        pathAndSensor[1] = timeseries.substring(lastPointIndex);
//    }

    public static String getType(String properties) {
        return properties.split(",")[0];
    }

    public static String getEncode(String properties) {
        return properties.split(",")[1];
    }

    public static String getPath(String timeseries) {
        int lastPointIndex = timeseries.lastIndexOf(".");
        return timeseries.substring(0, lastPointIndex);
    }

    public static String getSensor(String timeseries) {
        int lastPointIndex = timeseries.lastIndexOf(".");
        return timeseries.substring(lastPointIndex + 1);
    }

    public static void main(String[] argc) {

        String test = "INT32,RLE";
        String test2 = "root.excavator.Beijing.d1.s1";
        //System.out.println(splitPathAndSensor(test)[0] + " " + splitPathAndSensor(test)[1]);
        //System.out.println(getType(test) + " " + getEncode(test));
        System.out.println(getPath(test2) + " " + getSensor(test2));

    }
}
