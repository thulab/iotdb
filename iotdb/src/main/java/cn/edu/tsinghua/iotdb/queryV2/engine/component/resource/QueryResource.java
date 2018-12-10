package cn.edu.tsinghua.iotdb.queryV2.engine.component.resource;


public interface QueryResource {
    /**
     * Release represents the operations for current resource such as return, close, destroy
     */
    void release();
}
