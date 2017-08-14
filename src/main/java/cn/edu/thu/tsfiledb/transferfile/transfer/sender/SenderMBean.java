package cn.edu.thu.tsfiledb.transferfile.transfer.sender;

public interface SenderMBean {
	void transferNow();
	void switchMode(boolean mode);
	void changeTransferTimeInterval(long interval);
	long getTransferTimeInterval();
}
