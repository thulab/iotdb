package cn.edu.tsinghua.iotdb.sql;

import static org.junit.Assert.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.objenesis.strategy.InstantiatorStrategy;
import cn.edu.tsinghua.iotdb.qp.constant.DatetimeUtils;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;

public class DatetimeUtilsTest {
	private ZoneOffset zoneOffset;

	@Before
	public void setUp() throws Exception {
		zoneOffset =  ZonedDateTime.now().getOffset();
	}

	@After
	public void tearDown() throws Exception {
		System.out.println();
	}

	@Test
	public void testConvertDatetimeStrToLongWithoutMS() throws LogicalOperatorException {
		// 1546413207689
		// 2019-01-02T15:13:27.689+08:00
		String[] timeFormatWithoutMs = new String[]{
				"2019-01-02 15:13:27",
				"2019/01/02 15:13:27",
				"2019.01.02 15:13:27",
				"2019-01-02T15:13:27",
				"2019/01/02T15:13:27",
				"2019.01.02T15:13:27",
				"2019-01-02 15:13:27" + zoneOffset,
				"2019/01/02 15:13:27" + zoneOffset,
				"2019.01.02 15:13:27" + zoneOffset,
				"2019-01-02T15:13:27" + zoneOffset,
				"2019/01/02T15:13:27" + zoneOffset,
				"2019.01.02T15:13:27" + zoneOffset,
		};

		long res = 1546413207000L;
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToLong(str, zoneOffset));
		}
	}
	
	@Test
	public void testConvertDatetimeStrToLongWithMS() throws LogicalOperatorException {
		// 1546413207689
		// 2019-01-02T15:13:27.689+08:00
		String[] timeFormatWithoutMs = new String[]{
				"2019-01-02 15:13:27.689",
				"2019/01/02 15:13:27.689",
				"2019.01.02 15:13:27.689",
				"2019-01-02T15:13:27.689",
				"2019/01/02T15:13:27.689",
				"2019.01.02T15:13:27.689",
				"2019-01-02 15:13:27.689" + zoneOffset,
				"2019/01/02 15:13:27.689" + zoneOffset,
				"2019.01.02 15:13:27.689" + zoneOffset,
				"2019-01-02T15:13:27.689" + zoneOffset,
				"2019/01/02T15:13:27.689" + zoneOffset,
				"2019.01.02T15:13:27.689" + zoneOffset,
		};

		long res = 1546413207689L;
		for(String str : timeFormatWithoutMs) {
			assertEquals(res, DatetimeUtils.convertDatetimeStrToLong(str, zoneOffset));
		}
	}
	
	
	public void createTest() {
		long timestamp = System.currentTimeMillis();
		System.out.println(timestamp);
		ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("+08:00"));
		System.out.println(zonedDateTime);
	}
}


