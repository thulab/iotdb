package cn.edu.thu.tsfiledb.auth2.model;

import org.hamcrest.core.Is;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth2.exception.NoSuchPermException;

public class Permission {
	private static Logger logger = LoggerFactory.getLogger(Permission.class);
	public static final long NONE = 0l;
	public static final long READ = 1l;
	public static final long MODIFY = 1l << 1;
	public static final long CREATE = 1l << 2;
	public static final long INSERT = 1l << 3;
	public static final long DELETE = 1l << 4;
	public static final long ADMIN = -1;

	public static long combine(long perm1, long perm2) {
		return perm1 | perm2;
	}

	public static boolean test(long src, long target) {
		return (src & target) == target;
	}

	public static long revoke(long src, long target) {
		return src & (~target);
	}

	public static String longToName(long permission) {
		StringBuffer permStr = new StringBuffer();
		if (test(permission, READ)) {
			permStr.append("READ,");
		}
		if (test(permission, MODIFY)) {
			permStr.append("MODIFY,");
		}
		if (test(permission, CREATE)) {
			permStr.append("CREATE,");
		}
		if (test(permission, INSERT)) {
			permStr.append("INSERT,");
		}
		if (test(permission, DELETE)) {
			permStr.append("DELETE,");
		}
		if (permStr.length() > 0) {
			permStr.setLength(permStr.length() - 1);
		} else {
			permStr.append("NONE");
		}
		return permStr.toString();
	}

	public static long nameToLong(String name) throws NoSuchPermException {
		switch (name.toUpperCase()) {
		case "NONE":
			return NONE;
		case "READ":
			return READ;
		case "MODIFY":
			return MODIFY;
		case "INSERT":
			return INSERT;
		case "DELETE":
			return DELETE;
		case "CREATE":
			return CREATE;
		default:
			throw new NoSuchPermException(name + " is not supported");
		}
	}
}
