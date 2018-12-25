package cn.edu.tsinghua.iotdb.utils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommonUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);
	public static int getJDKVersion() {
    	String[] javaVersionElements = System.getProperty("java.version").split("\\.");
    	if(Integer.parseInt(javaVersionElements[0]) == 1) {
    		return Integer.parseInt(javaVersionElements[1]);
    	} else {
    		return Integer.parseInt(javaVersionElements[0]);
    	}
    }
	
//	public static void destroyBuffer(Buffer byteBuffer) throws Exception {
//		int javaVersion = getJDKVersion();
//		if (javaVersion == 8) {
//    		((DirectBuffer) byteBuffer).cleaner().clean();
//    	} else {
//    		try {
//            	final Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
//            	final Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
//            	theUnsafeField.setAccessible(true);
//            	final Object theUnsafe = theUnsafeField.get(null);
//            	final Method invokeCleanerMethod = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
//            	invokeCleanerMethod.invoke(theUnsafe, byteBuffer);
//    		} catch (Exception e) {
//    			throw e;
//    		}
//    	}
//	}

    private static Class clsDirectBuffer;
    private static MethodHandle mhDirectBufferCleaner;
    private static MethodHandle mhCleanerClean;

    static{
        try {
            clsDirectBuffer = Class.forName("sun.nio.ch.DirectBuffer");
            Method mDirectBufferCleaner = clsDirectBuffer.getMethod("cleaner");
            mhDirectBufferCleaner = MethodHandles.lookup().unreflect(mDirectBufferCleaner);
            Method mCleanerClean = mDirectBufferCleaner.getReturnType().getMethod("clean");
            mhCleanerClean = MethodHandles.lookup().unreflect(mCleanerClean);

            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            destroyBuffer(buf);
        }
        catch (Throwable t) {
        	LOGGER.error("FATAL: Cannot initialize optimized memory deallocator. Some data, both in-memory and on-disk, may live longer due to garbage collection.");
            throw new RuntimeException(t);
        }
    }

    public static void destroyBuffer(ByteBuffer buffer){
        if (buffer == null || !buffer.isDirect())
            return;
        try {
            Object cleaner = mhDirectBufferCleaner.bindTo(buffer).invoke();
            if (cleaner != null) {
                // ((DirectBuffer) buf).cleaner().clean();
                mhCleanerClean.bindTo(cleaner).invoke();
            }
        }
        catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
