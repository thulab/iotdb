package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import java.io.Serializable;

/**
 * Created by lylw on 2017/8/2.
 */
public class FilePositionRecord implements Serializable {
    String absolutePath;
    Long bytePosition;

    public FilePositionRecord(String absolutePath, Long bytePosition) {
        this.absolutePath = absolutePath;
        this.bytePosition = bytePosition;
    }

    public String getAbsolutePath() {
        return absolutePath;
    }

    public void setAbsolutePath(String absolutePath) {
        this.absolutePath = absolutePath;
    }

    public Long getBytePosition() {
        return bytePosition;
    }

    public void setBytePosition(Long bytePosition) {
        this.bytePosition = bytePosition;
    }
}
