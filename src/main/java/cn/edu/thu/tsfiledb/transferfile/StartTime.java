package cn.edu.thu.tsfiledb.transferfile;

import java.io.Serializable;

/**
 * Created by lylw on 2017/7/28.
 */
public class StartTime implements Serializable {
    private String device;
    private Long startTime;

    public StartTime(String device,long startTime){
        this.device=device.substring(0,device.length());
        this.startTime=startTime;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}
