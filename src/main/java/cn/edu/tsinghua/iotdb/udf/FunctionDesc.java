package cn.edu.tsinghua.iotdb.udf;

/**
 * @author qmm
 */
public class FunctionDesc {
  private final String functionName;
  private final String className;

  public FunctionDesc(String functionName, String className) {
    this.functionName = functionName;
    this.className = className;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }
}
