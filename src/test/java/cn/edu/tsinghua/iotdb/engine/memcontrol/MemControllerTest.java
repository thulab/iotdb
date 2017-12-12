package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.bufferwrite.BufferWriteProcessor;
import cn.edu.tsinghua.iotdb.exception.BufferWriteProcessorException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemControllerTest {

    TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();

    public static long GB = 1024 * 1024 * 1024L;
    public static long MB = 1024 * 1024L;

    @Test
    public void test() throws BufferWriteProcessorException {
        MemController memController = MemController.getInstance();
        memController.clear();

        Object[] dummyUser = new Object[20];
        for(int i = 0; i < dummyUser.length; i++)
            dummyUser[i] = new Object();

        // every one request 1 GB, should get 7 safes, 8 warning and 5 dangerous
        for(int i = 0; i < 7; i++) {
            MemController.UsageLevel level = memController.reportUse(dummyUser[i], 1 * GB);
            assertEquals(MemController.UsageLevel.SAFE, level);
        }
        for(int i = 7; i < 15; i++) {
            MemController.UsageLevel level = memController.reportUse(dummyUser[i], 1 * GB);
            assertEquals(MemController.UsageLevel.WARNING, level);
        }
        for(int i = 15; i < 20; i++) {
            MemController.UsageLevel level = memController.reportUse(dummyUser[i], 1 * GB);
            assertEquals(MemController.UsageLevel.DANGEROUS, level);
        }
        assertEquals(15 * GB, memController.getTotUsage());
        // every one free its mem
        for(int i = 0; i < 7; i++) {
            memController.reportFree(dummyUser[i], 1 * GB);
            assertEquals((14 - i) * GB, memController.getTotUsage());
        }
        for(int i = 7; i < 15; i++) {
            memController.reportFree(dummyUser[i], 2 * GB);
            assertEquals((14 - i) * GB, memController.getTotUsage());
        }
        // ask for a too big mem
        MemController.UsageLevel level = memController.reportUse(dummyUser[0], 100 * GB);
        assertEquals(MemController.UsageLevel.DANGEROUS, level);
        // single user ask continuously
        for(int i = 0; i < 8 * 1024 - 1; i++) {
            level = memController.reportUse(dummyUser[0], 1 * MB);
            assertEquals(MemController.UsageLevel.SAFE, level);
        }
        for(int i = 8 * 1024 - 1; i < 16 * 1024 - 1; i++) {
            level = memController.reportUse(dummyUser[0], 1 * MB);
            assertEquals(MemController.UsageLevel.WARNING, level);
        }
        for(int i = 16 * 1024 - 1; i < 17 * 1024; i++) {
            level = memController.reportUse(dummyUser[0], 1 * MB);
            System.out.println(memController.getTotUsage() / GB + " " + memController.getTotUsage() / MB % 1024);
            assertEquals(MemController.UsageLevel.DANGEROUS, level);
        }
    }
}
