package cn.edu.tsinghua.iotdb.index;

import cn.edu.tsinghua.iotdb.index.common.DataFileInfo;
import cn.edu.tsinghua.iotdb.index.common.IndexManagerException;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;

import java.util.List;
import java.util.Map;

/**
 * 索引本身是单例的.
 */
public interface IoTIndex {

    /**
     * 初始化索引，digest要初始化一些内存中的节点，KVIndex不做任何事
     */
    void init();

    /**
     * 为指定文件构建索引。注意，参数中给出了现有的文件列表，索引自己可以决定是
     * "完全更新"，还是增量式更新并删除不再存在的文件（KVIndex的做法）。
     * 这一过程可以是异步的，对于异步的构建索引，立即返回然后开始建立索引，如果此时来了查询，简单的做法是直接当做没有索引来处理
     *
     * @param path       待修改的索引的path
     * @param fileList   现有的固化的文件列表，把索引按照这些文件更新一遍；
     * @param parameters 其他参数，可选
     * @return whether the operation is successful
     * @throws IndexManagerException
     */
    boolean build(Path path, List<DataFileInfo> fileList, Map<String, Object> parameters)
            throws IndexManagerException;

    /**
     * 与上面唯一的不同在于，只新加了一个文件。两个函数区别开是因为，上面的函数多用于完全新建或者merge的时候，
     * 而这个发生在关掉一个fileNode形成新文件的时候。
     * 这一过程可以是异步的，对于异步的构建索引，立即返回然后开始建立索引，如果此时来了查询，简单的做法是直接当做没有索引来处理
     *
     * @param path       待修改的索引的path
     * @param newFile    新加的一个文件；
     * @param parameters 其他参数，可选
     * @return
     * @throws IndexManagerException
     */
    boolean build(Path path, DataFileInfo newFile, Map<String, Object> parameters)
            throws IndexManagerException;

    /**
     * Given the new file list after merge, delete all index files which are not in the list,
     * and switch to the new index files along with the new data files.
     * Call this method after the merge operation has completed. Block index read and write during this process.
     *
     * @param newFileList the data files leaves after the merge operation, the column paths in the file list need to
     *                    build index, some one may has no data in some data file
     * @return whether the operation is successful
     * @throws IndexManagerException if the given column path is not correct or some base service occurred error
     */
    boolean mergeSwitch(Path path, List<DataFileInfo> newFileList) throws IndexManagerException;

    /**
     * 末位添加，只用于实时索引
     *
     * @param path
     * @param timestamp
     * @param value
     */
    void append(Path path, long timestamp, String value);

    /**
     * 单点更新，只用于实时索引
     *
     * @param path
     * @param timestamp
     * @param value
     */
    void update(Path path, long timestamp, String value);

    /**
     * 区间段更新，只用于实时索引
     *
     * @param path
     * @param starttime
     * @param endtime
     * @param value
     */
    void update(Path path, long starttime, long endtime, String value);

    /**
     * 某个时间点前的删除，只用于实时索引
     *
     * @param path
     * @param timestamp
     */
    void delete(Path path, long timestamp);

    /**
     * 将整个索引关闭，如果在内存中有一些信息，则要序列化到磁盘上。注意，这一步需要是同步的。
     *
     * @return 是否正确完成
     * @throws IndexManagerException
     */
    boolean close() throws IndexManagerException;

    /**
     * 彻底删除一个path的索引
     *
     * @param path the column path
     * @return whether the operation is successful
     * @throws IndexManagerException
     */
    boolean drop(Path path) throws IndexManagerException;

    /**
     * 输入查询请求和未被更改过的区间，以及单次返回的数量，查询。
     * 这个查询针对于功能式索引，返回的结果就是要显示给用户的结果
     * 插件式索引由于自带新功能，所以无论是否见好索引，都应该返回预期的结果（可以是没有建好索引的部分就暴力搜索，也可以返回"无可奉告"）
     *
     * @param path               the path to be queried
     * @param parameters         the query request with all parameters
     * @param nonUpdateIntervals the query request with all parameters
     * @param limitSize          the limitation of number of answers
     * @return the query response
     */
    Object query(Path path, List<Object> parameters, List<Pair<Long, Long>> nonUpdateIntervals, int limitSize)
            throws IndexManagerException;
}