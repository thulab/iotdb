package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.aop.CostResult;

public class Demo2 {

    public static void main(String[] args) {

        Any any = new Any();
        Any any2 = new Any();

        CostResult.getInstance().startRecord();

        System.out.println(any.show2());
        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));
        System.out.println(any2.show2());
        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));


//        System.out.println(any.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//        System.out.println(any2.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//
//
//        System.out.println(any.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//        System.out.println(any2.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//
//        System.out.println(any.show2());
//        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));
//        System.out.println(any2.show2());
//        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));
//
//        CostResult.getInstance().stopRecord();
//
//        System.out.println(any.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//        System.out.println(any2.show());
//        System.out.println("false:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show"));
//
//        System.out.println(any.show2());
//        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));
//        System.out.println(any2.show2());
//        System.out.println("true:" + CostResult.getInstance().getTotalTimeCost("cn/edu/tsinghua/iotdb/performance/Any", "show2"));

    }
}
