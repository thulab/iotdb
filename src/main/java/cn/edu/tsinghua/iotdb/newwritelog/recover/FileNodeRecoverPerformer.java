package cn.edu.tsinghua.iotdb.newwritelog.recover;

import cn.edu.tsinghua.iotdb.exception.RecoverException;

public class FileNodeRecoverPerformer implements RecoverPerfromer {

    /**
     * If the storage group is set at "root.a.b", then the identifier for a bufferwrite processor will be "root.a.b-bufferwrite",
     * and the identifier for an overflow processor will be "root.a.b-overflow".
     */
    private String identifier;

    public FileNodeRecoverPerformer(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public void recover() throws RecoverException {
        // TODO : implement this
    }
}
