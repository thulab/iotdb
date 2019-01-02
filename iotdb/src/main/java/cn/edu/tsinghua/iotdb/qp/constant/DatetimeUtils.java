package cn.edu.tsinghua.iotdb.qp.constant;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import cn.edu.tsinghua.iotdb.qp.exception.LogicalOperatorException;

public class DatetimeUtils {
	/**
	 * such as '2011/12/03'.
	 */
    public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_SLASH;
    static {
    	ISO_LOCAL_DATE_WITH_SLASH = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .appendLiteral('/')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('/')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();
    }
    
    /**
	 * such as '2011.12.03'.
	 */
    public static final DateTimeFormatter ISO_LOCAL_DATE_WITH_DOT;
    static {
    	ISO_LOCAL_DATE_WITH_DOT = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                .appendLiteral('.')
                .appendValue(ChronoField.MONTH_OF_YEAR, 2)
                .appendLiteral('.')
                .appendValue(ChronoField.DAY_OF_MONTH, 2)
                .toFormatter();
    }

    /**
	 * such as '10:15:30' or '10:15:30.123'.
	 */
    public static final DateTimeFormatter ISO_LOCAL_TIME_WITH_MS;
    static {
    	ISO_LOCAL_TIME_WITH_MS = new DateTimeFormatterBuilder()
                .appendValue(ChronoField.HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                .optionalStart()
                .appendLiteral('.')
                .appendValue(ChronoField.MILLI_OF_SECOND, 3)
                .optionalEnd()
                .toFormatter();
    }
    
	/**
	 * such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_MS;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_MS = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter();
    }
    
    
	/**
	 * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_SLASH = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter(); 
    }

	/**
	 * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_DOT = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral('T')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter(); 
    }

	/**
	 * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SPACE;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_SPACE = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(DateTimeFormatter.ISO_LOCAL_DATE)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter(); 
    }

	/**
	 * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_SLASH)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter(); 
    }
    
	/**
	 * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
	 */
    public static final DateTimeFormatter ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE;
    static {
    	ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE = new DateTimeFormatterBuilder()
                .parseCaseInsensitive()
                .append(ISO_LOCAL_DATE_WITH_DOT)
                .appendLiteral(' ')
                .append(ISO_LOCAL_TIME_WITH_MS)
                .appendOffsetId()
                .toFormatter(); 
    }
    
    public static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()		
			/**
			 * The ISO date-time formatter that formats or parses a date-time with an
			 * offset, such as '2011-12-03T10:15:30+01:00' or '2011-12-03T10:15:30.123+01:00'.
			 */
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_MS)
			/**
			 * such as '2011/12/03T10:15:30+01:00' or '2011/12/03T10:15:30.123+01:00'.
			 */	
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH)
			/**
			 * such as '2011.12.03T10:15:30+01:00' or '2011.12.03T10:15:30.123+01:00'.
			 */	
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT)
			/**
			 * such as '2011-12-03 10:15:30+01:00' or '2011-12-03 10:15:30.123+01:00'.
			 */			
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_SPACE)
			/**
			 * such as '2011/12/03 10:15:30+01:00' or '2011/12/03 10:15:30.123+01:00'.
			 */	
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_SLASH_WITH_SPACE)
			/**
			 * such as '2011.12.03 10:15:30+01:00' or '2011.12.03 10:15:30.123+01:00'.
			 */	
			.appendOptional(ISO_OFFSET_DATE_TIME_WITH_DOT_WITH_SPACE)
			.toFormatter();
	
	
	public static long convertDatetimeStrToLong(String str, ZoneOffset offset) throws LogicalOperatorException {
		if(str.length() - str.lastIndexOf('+') != 6 && str.length() - str.lastIndexOf('-') != 6) {
			return convertDatetimeStrToLong(str+offset, offset);
		}
		ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
		return zonedDateTime.toInstant().toEpochMilli();
	}
}
