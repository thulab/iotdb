package cn.edu.thu.tsfiledb.auth2.model;

public class Permission {
	public static final long READ = 1l;
	public static final long WRITE = 1l << 1 ;
	
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
		if(test(permission, READ)) {
			permStr.append("READ,");
		}
		if(test(permission, WRITE)) {
			permStr.append("WRITE,");
		}
		if (permStr.length() > 0) {
			permStr.setLength(permStr.length() - 1);
		}
		return permStr.toString();
	}
}
