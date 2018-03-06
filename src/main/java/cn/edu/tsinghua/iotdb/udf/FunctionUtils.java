package cn.edu.tsinghua.iotdb.udf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qmm
 */
public class FunctionUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionUtils.class);

  /**
   * @param addedJars separate by semicolon
   */
  public static void addJars(String addedJars) {
    ClassLoader loader = getClassLoader();
    ClassLoader newLoader = addToClassPath(loader, addedJars.split(";"));
    Thread.currentThread().setContextClassLoader(newLoader);
  }

  private static ClassLoader addToClassPath(ClassLoader loader, String[] newPaths) {
    final URLClassLoader urlClassLoader = (URLClassLoader) loader;
    if (urlClassLoader instanceof UDFClassLoader) {
      final UDFClassLoader udfClassLoader = (UDFClassLoader) urlClassLoader;
      for (String path : newPaths) {
        udfClassLoader.addURL(urlFromPathString(path));
      }
      return udfClassLoader;
    } else {
      return createUDFClassLoader(urlClassLoader, newPaths);
    }
  }

  private static ClassLoader createUDFClassLoader(URLClassLoader loader, String[] newPaths) {
    final Set<URL> curPathsSet = Sets.newHashSet(loader.getURLs());
    final List<URL> curPaths = Lists.newArrayList(curPathsSet);
    for (String path : newPaths) {
      final URL url = urlFromPathString(path);
      if (url != null && !curPathsSet.contains(url)) {
        curPaths.add(url);
      }
    }
    return new UDFClassLoader(curPaths.toArray(new URL[0]), loader);
  }

  public static ClassLoader getClassLoader() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = FunctionUtils.class.getClassLoader();
    }
    return classLoader;
  }

  private static URL urlFromPathString(String path) {
    URL url = null;
    try {
      if (StringUtils.indexOf(path, "file:/") == 0) {
        url = new URL(path);
      } else {
        url = new File(path).toURI().toURL();
      }
    } catch (Exception err) {
      LOGGER.error("Bad URL {}, ignoring path", path);
    }
    return url;
  }
}
