package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfiledb.exception.PathErrorException;

import java.io.File;
import java.util.List;

/**
 * This is the interface of index managers.
 *
 * @author Jiaye Wu
 */
public interface IndexManager {

    boolean build(String nameSpacePath) throws PathErrorException;

    boolean build(String nameSpacePath, long sinceTime) throws PathErrorException;

    QueryResponse query(QueryRequest queryRequest);

    boolean rebuild(List<File> fileList);
}
