package cn.edu.tsinghua.iotdb.postback.sender;

import java.util.Set;
/**
 * @author lta
 */
public interface FileSender {
	/**
	 * Connect to server
	 */
	void connection(String serverIp, int serverPort);
	
	/**
	 * Transfer UUID to receiver
	 */
	public boolean transferUUID(String uuidPath);

	/**
	 * Make file snapshots before sending files
	 */
	public Set<String> makeFileSnapshot(Set<String> sendingFileList);

	/**
	 * Send schema file to receiver.
	 */
	public void sendSchema(String schemaPath);

	/**
	 * For each file in fileList, send it to receiver side
	 * @param fileList:file list to send
	 */
	public void startSending(Set<String> fileSnapshotList); 

	/**
	 * Close socket after send files
	 */
	public boolean afterSending();
	
	/**
	 * Execute a postback task.
	 */
	public void postback();

}
