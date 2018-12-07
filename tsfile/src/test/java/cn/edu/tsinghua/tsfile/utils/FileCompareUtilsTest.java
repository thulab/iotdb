package cn.edu.tsinghua.tsfile.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import cn.edu.tsinghua.tsfile.constant.TimeseriesTestConstant;

/**
 * 
 * @author kangrong
 *
 */
public class FileCompareUtilsTest {

    @Test
    public void testConvertUnit() {
        long kb = 3 * 1024;
        long mb = kb * 1024;
        long gb = mb * 1024;
        assertEquals(3.0 * 1024, FileUtils.transformUnit(kb, FileUtils.Unit.B),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(kb, FileUtils.Unit.KB),
                TimeseriesTestConstant.double_min_delta);

        assertEquals(3, FileUtils.transformUnit(mb, FileUtils.Unit.MB),
                TimeseriesTestConstant.double_min_delta);
        assertEquals(3, FileUtils.transformUnit(gb, FileUtils.Unit.GB),
                TimeseriesTestConstant.double_min_delta);
    }

    @Test
    public void testConvertToByte() {
        assertEquals(3l, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.B));
        assertEquals(3l * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.KB));
        assertEquals(3l * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.MB));
        assertEquals(3l * 1024 * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.GB));
    }

}
