package cn.edu.tsinghua.tsfile.timeseries.filter;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.filter.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filter.factory.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import org.junit.Assert;
import org.junit.Test;


public class OperatorTest {
    private static final long EFFICIENCY_TEST_COUNT = 10000000;
    private static final long TESTED_TIMESTAMP = 1513585371L;

    @Test
    public void testEq() {
        Filter timeEq = TimeFilter.eq(100L);
        Assert.assertEquals(true, timeEq.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeEq.satisfy(
                new TimeValuePair(101, new TsPrimitiveType.TsInt(100))));

        Filter filter2 = FilterFactory.and(TimeFilter.eq(100L), ValueFilter.eq(50));
        Assert.assertEquals(true, filter2.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(50))));
        Assert.assertEquals(false, filter2.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(51))));

        Filter filter3 = ValueFilter.eq(true);
        Assert.assertEquals(true, filter3.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsBoolean(true))));
        Assert.assertEquals(false, filter3.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsBoolean(false))));
    }

    @Test
    public void testGt() {
        Filter timeGt = TimeFilter.gt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100))));

        Filter valueGt = ValueFilter.gt(0.01f);
        Assert.assertEquals(true, valueGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(0.02f))));
        Assert.assertEquals(false, valueGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(0.01f))));
        Assert.assertEquals(false, valueGt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsFloat(-0.01f))));

        Filter binaryFilter = ValueFilter.gt(new Binary("test1"));
        Assert.assertEquals(true, binaryFilter.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsBinary(new Binary("test2")))));
        Assert.assertEquals(false, binaryFilter.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsBinary(new Binary("test0")))));
    }

    @Test
    public void testGtEq() {
        Filter timeGtEq = TimeFilter.gtEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(true, timeGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100))));

        Filter valueGtEq = ValueFilter.gtEq(0.01);
        Assert.assertEquals(true, valueGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(0.02))));
        Assert.assertEquals(true, valueGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(0.01))));
        Assert.assertEquals(false, valueGtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsDouble(-0.01))));
    }

    @Test
    public void testLt() {
        Filter timeLt = TimeFilter.lt(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100))));

        Filter valueLt = ValueFilter.lt(100L);
        Assert.assertEquals(true, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L))));
        Assert.assertEquals(false, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L))));
        Assert.assertEquals(false, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L))));
    }

    @Test
    public void testLtEq() {
        Filter timeLtEq = TimeFilter.ltEq(TESTED_TIMESTAMP);
        Assert.assertEquals(true, timeLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(true, timeLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(false, timeLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100))));

        Filter valueLtEq = ValueFilter.ltEq(100L);
        Assert.assertEquals(true, valueLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L))));
        Assert.assertEquals(true, valueLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L))));
        Assert.assertEquals(false, valueLtEq.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L))));
    }


    @Test
    public void testNot() {
        Filter timeLt = TimeFilter.not(TimeFilter.lt(TESTED_TIMESTAMP));
        Assert.assertEquals(false, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP - 1, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(true, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(true, timeLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP + 1, new TsPrimitiveType.TsInt(100))));

        Filter valueLt = ValueFilter.not(ValueFilter.lt(100L));
        Assert.assertEquals(false, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(99L))));
        Assert.assertEquals(true, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(100L))));
        Assert.assertEquals(true, valueLt.satisfy(
                new TimeValuePair(TESTED_TIMESTAMP, new TsPrimitiveType.TsLong(101L))));
    }

    @Test
    public void testNotEq() {
        Filter timeNotEq = TimeFilter.notEq(100L);
        Assert.assertEquals(false, timeNotEq.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(100))));
        Assert.assertEquals(true, timeNotEq.satisfy(
                new TimeValuePair(101, new TsPrimitiveType.TsInt(100))));

        Filter valueNotEq = ValueFilter.notEq(50);
        Assert.assertEquals(false, valueNotEq.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(50))));
        Assert.assertEquals(true, valueNotEq.satisfy(
                new TimeValuePair(100, new TsPrimitiveType.TsInt(51))));
    }

    @Test
    public void testAndOr() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Assert.assertEquals(true, andFilter.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50))));
        Assert.assertEquals(false, andFilter.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60))));
        Assert.assertEquals(false, andFilter.satisfy(
                new TimeValuePair(99L, new TsPrimitiveType.TsDouble(50))));

        Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));
        Assert.assertEquals(true, orFilter.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50))));
        Assert.assertEquals(false, orFilter.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60))));
        Assert.assertEquals(true, orFilter.satisfy(
                new TimeValuePair(1000L, new TsPrimitiveType.TsDouble(50))));

        Filter andFilter2 = FilterFactory.and(orFilter, ValueFilter.notEq(50.0));
        Assert.assertEquals(false, andFilter2.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(50))));
        Assert.assertEquals(false, andFilter2.satisfy(
                new TimeValuePair(101L, new TsPrimitiveType.TsDouble(60))));
        Assert.assertEquals(true, andFilter2.satisfy(
                new TimeValuePair(1000L, new TsPrimitiveType.TsDouble(51))));
    }

    @Test
    public void testWrongUsage() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(true));
        TimeValuePair timeValuePair = new TimeValuePair(101L, new TsPrimitiveType.TsLong(50));
        try {
            andFilter.satisfy(timeValuePair);
            Assert.fail();
        }catch (ClassCastException e){

        }
    }

    @Test
    public void efficiencyTest() {
        Filter andFilter = FilterFactory.and(TimeFilter.gt(100L), ValueFilter.lt(50.9));
        Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(1000L));

        long startTime = System.currentTimeMillis();
        for (long i = 0; i < EFFICIENCY_TEST_COUNT; i++) {
            TimeValuePair tvPair = new TimeValuePair(Long.valueOf(i), new TsPrimitiveType.TsDouble(i + 0.1));
            orFilter.satisfy(tvPair);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("EfficiencyTest for Filter: \n\tFilter Expression = " + orFilter + "\n\tCOUNT = " + EFFICIENCY_TEST_COUNT +
                "\n\tTotal Time = " + (endTime - startTime) + "ms.");
    }
}
