package cn.edu.tsinghua.iotdb.performance;

import cn.edu.tsinghua.aop.Cost;

public class Any {


    @Cost
    public long show() {
        long time=System.currentTimeMillis();
        double a=0;
        for(int i=0;i<10000000;i++) {
            a+=Math.random();
        }
        return System.currentTimeMillis()-time;
    }

    @Cost
    public long show2() {
        long time =System.currentTimeMillis();
        double a=0;
        for(int i=0;i<20000000;i++) {
            a+=Math.random();
        }
        return System.currentTimeMillis()-time;
    }
}