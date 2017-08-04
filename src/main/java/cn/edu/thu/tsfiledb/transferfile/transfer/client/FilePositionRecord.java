package cn.edu.thu.tsfiledb.transferfile.transfer.client;

import java.io.Serializable;

/**
 * Created by lylw on 2017/8/2.
 */
public class FilePositionRecord implements Serializable {
	private static final long serialVersionUID = 2314510818613179964L;
	private String absolutePath;
    private long bytePosition;

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

    public long getBytePosition() {
        return bytePosition;
    }

    public void setBytePosition(long bytePosition) {
        this.bytePosition = bytePosition;
    }
}
