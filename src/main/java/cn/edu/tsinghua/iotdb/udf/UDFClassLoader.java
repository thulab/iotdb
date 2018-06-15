package cn.edu.tsinghua.iotdb.udf;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @author qmm
 */
public class UDFClassLoader extends URLClassLoader {

  public UDFClassLoader(URL[] urls) {
    super(urls);
  }

  public UDFClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected void addURL(URL url) {
    super.addURL(url);
  }
}