package cn.edu.thu.tsfiledb.index;

import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.exception.IndexManagerException;

import java.util.List;

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
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean build(Path columnPath, long sinceTime) throws IndexManagerException;

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
     * @param columnPath       the column path
     * @param modifiedFileList the data files have been modified or created in the merge/close operation
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean rebuild(Path columnPath, List<DataFileInfo> modifiedFileList) throws IndexManagerException;

    /**
     * Given the new file list after merge/close, delete all index files which are not in the list,
     * and switch to the new index files along with the new data files.
     * Call this method after the merge/close operation has completed. Block index read and write during this process.
     *
     * @param columnPath  the column path
     * @param newFileList new file list after merge/close
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean switchIndexes(Path columnPath, List<DataFileInfo> newFileList) throws IndexManagerException;

    /**
     * Query index for result.
     * All modified/new time intervals need to be scanned in brute-force method in order to keep no false dismissals.
     *
     * @param queryRequest the query request with all parameters
     * @return the query response
     */
    QueryResponse query(QueryRequest queryRequest) throws IndexManagerException;
}
