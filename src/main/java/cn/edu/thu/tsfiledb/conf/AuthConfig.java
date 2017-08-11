package cn.edu.thu.tsfiledb.conf;

import java.io.File;

public class AuthConfig {

	/**
	 *  This means the spaced used by a PerTreeNode in byte.
	 *  Decrease this will help saving space, but will potentially increase
	 *  access time.
	 *  It is recommended this should be no less than 4096 (4KB), and 
	 *  be a integral multiple of 4096, for that is usually a disk / memory page.
	 *  PAGE_SIZE should > PERM_TREE_HEADER_SIZE
	 */
	public final int PAGE_SIZE = 40960; 
	
	public final int PERM_TREE_HEADER_SIZE = 960;
	
	/**
	 * This constrains the max name length in a level of the path or a user.
	 * Decrease this will enable a single node to store more info, 
	 * resulting in less space consume.
	 * TODO check name length when a node is created.
	 */
	public final int MAX_NODENAME_LENGTH = 256;
	
	public final int MAX_USERNAME_LENGTH = 256;
	
	public final int MAX_PW_LENGTH = 64;
	
	// locations of auth db files
	public final String AUTH_FOLDER = "auth" + File.separator;
	
	public final String SUPER_USER = "root";
	
	public final String PERM_FOLDER = AUTH_FOLDER + "perm" + File.separator;
	
	public final String PERMFILE_SUFFIX = ".perm";
	
	public final String PERMMETA_SUFFIX = ".perm.meta";
	
	public final String ROLE_FOLDER = AUTH_FOLDER + "role" + File.separator;
	
	public final String ROLE_INFO_FILE = "roleInfo";
	
	public final String ROLE_META_FILE = "roleInfo.meta";
	
	public final String USER_FOLDER = AUTH_FOLDER + "user" + File.separator;
	
	public final String USER_INFO_FILE = "userInfo";
	
	public final String USER_META_FILE = "userInfo.meta";
	
	// cache settings
	public final int PERM_CACHE_CAPACITY = 1000;
	
	public final int NODE_CACHE_CAPACITY = 1000;
	
	// random access file open mode
	public final String RAF_READ = "r";
	
	public final String RAF_READ_WRITE = "rw";

	}
