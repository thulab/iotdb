package cn.edu.tsinghua.iotdb.postback.receiver;

import java.io.File;
import java.text.DecimalFormat;

public class Utils {

	/**
	 * Verify IP address with IP white list which contains more than one IP segment.
	 * @param IPwhiteList
	 * @param IPaddress
	 * @return
	 */
	public static boolean verifyIPSegment(String IPwhiteList, String IPaddress) {
		String[] IPsegments = IPwhiteList.split(",");
		for(String IPsegment:IPsegments) {
			int subnetMask = Integer.parseInt(IPsegment.substring(IPsegment.indexOf("/") + 1));
			IPsegment = IPsegment.substring(0, IPsegment.indexOf("/"));
			if(verifyIP(IPsegment, IPaddress, subnetMask))
				return true;
		}
		return false;
	}
	
	/**
	 * Verify IP address with IP segment.
	 * @param IPsegment
	 * @param IPaddress
	 * @param subnetMark
	 * @return
	 */
	private static boolean verifyIP(String IPsegment, String IPaddress, int subnetMark) {
		String IPsegmentBinary = "";
		String IPaddressBinary = "";
		String[] IPsplits = IPsegment.split("\\.");
		DecimalFormat df=new DecimalFormat("00000000");
		for(String IPsplit:IPsplits) {
			IPsegmentBinary = IPsegmentBinary + String.valueOf(df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
		}
		IPsegmentBinary = IPsegmentBinary.substring(0, subnetMark);
		IPsplits = IPaddress.split("\\.");
		for(String IPsplit:IPsplits) {
			IPaddressBinary = IPaddressBinary + String.valueOf(df.format(Integer.parseInt(Integer.toBinaryString(Integer.parseInt(IPsplit)))));
		}
		IPaddressBinary = IPaddressBinary.substring(0, subnetMark);
		if(IPaddressBinary.equals(IPsegmentBinary))
			return true;
		else
			return false;
	}


	public static void deleteFile(File file) {
		if (file.isFile() || file.list().length == 0) {
			file.delete();
		} else {
			File[] files = file.listFiles();
			for (File f : files) {
				deleteFile(f);
				f.delete();
			}
		}
	}
}
