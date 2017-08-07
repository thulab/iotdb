package cn.edu.thu.tsfiledb.auth2.model;

public class Permission {
	public static long READ = 1l;
	public static long write = 1l << 1 ;
	
	public static long combine(long perm1, long perm2) {
		return perm1 | perm2;
	}
	
	public static boolean test(long src, long target) {
		return (src & target) == target;
	}
}
