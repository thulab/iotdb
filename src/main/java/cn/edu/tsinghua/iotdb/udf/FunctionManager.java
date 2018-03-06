package cn.edu.tsinghua.iotdb.udf;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qmm
 */
public class FunctionManager {

  private static final Logger logger = LoggerFactory.getLogger(FunctionManager.class);

  private final Map<String, FunctionDesc> functions;

  public FunctionManager() {
    functions = new HashMap<>();
    // register built-in UDSF
    functions.put("timeSpan", new FunctionDesc("timeSpan", "cn.edu.tsinghua.iotdb.udf.TimeSpan"));
  }

  public void createTemporaryFunction(FunctionDesc desc) {
    try {
      Class<?> udfClass = getUdfClass(desc);
      if (!isLegal(udfClass)) {
        logger.error("FAILED: Class {} does not implement UDSF", desc.getClassName());
      }
      functions.put(desc.getFunctionName(), desc);
    } catch (ClassNotFoundException e) {
      logger.error("FAILED: Class {} not found", desc.getClassName());
    }
  }

  public Map<String, FunctionDesc> getFunctions() {
    return functions;
  }

  private Class<?> getUdfClass(FunctionDesc desc) throws ClassNotFoundException {
    ClassLoader classLoader = FunctionUtils.getClassLoader();
    return Class.forName(desc.getClassName(), false, classLoader);
  }

  private boolean isLegal(Class<?> udfClass) {
    return udfClass.getSuperclass().getName().equals("UDSF");
  }
}
