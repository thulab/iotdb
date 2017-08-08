package cn.edu.thu.tsfiledb.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.exception.PathErrorException;

public class PathUtils {
	private static Logger logger = LoggerFactory.getLogger(PathUtils.class);
	
	public static String[] getPathLevels(String path) throws PathErrorException {
		if(path == null) {
			logger.error("null path");
			throw new PathErrorException("null path");
		}
		String[] pathLevels = path.split("\\.");
		if(!pathLevels[0].equals("root")) {
			logger.error("{} is a illegal path", path);
			throw new PathErrorException("first level is {} instead of root"); 
		}
		return pathLevels;
	}
}
