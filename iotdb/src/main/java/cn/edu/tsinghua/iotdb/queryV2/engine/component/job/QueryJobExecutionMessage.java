package cn.edu.tsinghua.iotdb.queryV2.engine.component.job;


public class QueryJobExecutionMessage {

    private String message;

    public QueryJobExecutionMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
