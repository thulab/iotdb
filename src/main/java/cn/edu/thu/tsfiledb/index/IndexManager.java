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
     * Build index for data after specific time of the given column path.
     *
     * @param columnPath the column path
     * @param parameters the parameters used to build index, include build since time, window length etc.
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean build(Path columnPath, Map<String, Object> parameters) throws IndexManagerException;

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
     * Build index for data in the file, which should be a new data file flushed by close operation.
     * This process should be asynchronous, and not influence the close operation to complete immediately.
     *
     * @param columnPaths    the column paths in the file need to build index, some one may has no data in the data file
     * @param newFile        the data file created in the close operation
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean closeBuild(List<Path> columnPaths, DataFileInfo newFile) throws IndexManagerException;

    /**
     * Build index for data in the file list, and not overwrite exist ones,
     * pending for merge manager to call the {@link this.mergeSwitch()} method to switch index files.
     *
     * @param columnPaths    the column paths in the file list need to build index, some one may has no data in some data file
     * @param newFileList    the data files leaves after the merge operation
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean mergeBuild(List<Path> columnPaths, List<DataFileInfo> newFileList) throws IndexManagerException;

    /**
     * Given the new file list after merge, delete all index files which are not in the list,
     * and switch to the new index files along with the new data files.
     * Call this method after the merge operation has completed. Block index read and write during this process.
     *
     * @param columnPaths    the column paths in the file list need to build index, some one may has no data in some data file
     * @param newFileList    new file list after the merge operation
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean mergeSwitch(List<Path> columnPaths, List<DataFileInfo> newFileList) throws IndexManagerException;

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
