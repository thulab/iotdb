package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;

import java.util.List;
import java.util.Map;

/**
 * This is the interface of index managers.
 *
 * @author Jiaye Wu
 */
public interface IndexManager {

    /**
     * Build index for all data of the given column path.
     *
     * @param columnPath the column path
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct
     */
    boolean build(Path columnPath) throws IndexManagerException;

    /**
     * Build index for data after specific time of the given column path.
     *
     * @param columnPath the column path
     * @param sinceTime  only build index for data after this time
     * @param parameters the parameters used to building index
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean build(Path columnPath, long sinceTime, Map<String, Integer> parameters) throws IndexManagerException;

    /**
     * Delete all index files of the given column path.
     * Used for dropping index operation.
     *
     * @param columnPath the column path
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean delete(Path columnPath) throws IndexManagerException;

    /**
     * Build index for data in the file list, and not overwrite exist ones,
     * pending for merge/close manager to call the switchIndexes() method to switch index files.
     *
     * @param columnPaths      the column paths
     * @param modifiedFileList the data files have been modified or created in the merge/close operation
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean rebuild(List<Path> columnPaths, List<DataFileInfo> modifiedFileList) throws IndexManagerException;

    /**
     * Given the new file list after merge/close, delete all index files which are not in the list,
     * and switch to the new index files along with the new data files.
     * Call this method after the merge/close operation has completed. Block index read and write during this process.
     *
     * @param columnPaths the column paths
     * @param newFileList new file list after merge/close
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean switchIndexes(List<Path> columnPaths, List<DataFileInfo> newFileList) throws IndexManagerException;

    /**
     * Query index for result.
     * All modified/new time intervals need to be scanned in brute-force method in order to keep no false dismissals.
     *
     * @param queryRequest the query request with all parameters
     * @param limitSize    the limitation of number of answers
     * @return the query response
     */
    QueryDataSet query(QueryRequest queryRequest, int limitSize) throws IndexManagerException;
}
