//package cn.edu.tsinghua.iotdb.queryV2.reader;
//
//import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PriorityMergeReader;
//import cn.edu.tsinghua.iotdb.queryV2.reader.merge.PrioritySeriesReader;
//import cn.edu.tsinghua.iotdb.queryV2.reader.unsequence.UnSeqSeriesReader;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.io.IOException;
//
//
//public class UnSeqSeriesReaderTest {
//
//    @Test
//    public void testPeek() throws IOException {
//        long[] ret1 = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
//        test(ret1);
//        long[] ret2 = new long[]{1};
//        test(ret2);
//        long[] ret3 = new long[]{};
//        test(ret3);
//    }
//
//    public void test(long[] ret) throws IOException {
//        SeriesMergeSortReaderTest.FakedSeriesReader fakedSeriesReader = new SeriesMergeSortReaderTest.FakedSeriesReader(
//                ret);
//        UnSeqSeriesReader unSeqSeriesReader = new UnSeqSeriesReader(1L,
//                new PriorityMergeReader(new PrioritySeriesReader(fakedSeriesReader, new PrioritySeriesReader.Priority(1))));
//        for (int i = 0; i < ret.length; i++) {
//            for (int j = 0; j < 10; j++) {
//                Assert.assertEquals(ret[i], unSeqSeriesReader.peek().getTimestamp());
//            }
//            Assert.assertEquals(ret[i], unSeqSeriesReader.next().getTimestamp());
//        }
//    }
//}
