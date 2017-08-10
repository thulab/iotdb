package cn.edu.thu.tsfiledb.auth2.manage;

public class AuthConfig {
	public static String authFolder = "auth/";
	public static String roleFolder = authFolder + "role/";
	public static String userFolder = authFolder + "user/";
	public static String permFolder = authFolder + "perm/";

	/**
	 *  This means the spaced used by a PerTreeNode in byte.
	 *  Decrease this will help saving space, but will potentially increase
	 *  access time.
	 *  It is recommended this should be no less than 4096 (4KB), and 
	 *  be a integral multiple of 4096, for that is usually a disk / memory page.
	 */
	public static final int PAGE_SIZE = 40960; 
	/**
	 * This constrains the max name length in a level of the path.
	 * Decrease this will enable a single node to store more info, 
	 * resulting in less space consume.
	 * TODO check name length when a node is created.
	 */
	public static final int MAX_NODENAME_LENGTH = 256;
}
