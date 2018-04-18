package cn.edu.tsinghua.iotdb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class TsfileMetadataResultSet extends TsfileQueryResultSet {

	private Iterator<?> columnItr;

	private MetadataType type;

	private ColumnSchema currentColumn;

	private String currentDeltaObjectOrStorageGroup;

	private List<String> currentTimeseries; // current row for show timeseries

	private int colCount; // the number of columns for show
	private String[] showLabels; // headers for show
	private int[] maxValueLength; // the max length of a column for show

	private static final String GET_STRING_DELTA_OBJECT_OR_STORAGE_GROUP = "DELTA_OBJECT_OR_STORAGE_GROUP";
	private static final String GET_STRING_COLUMN_NAME= "COLUMN_NAME";
	private static final String GET_STRING_COLUMN_TYPE= "COLUMN_TYPE";
	private static final String GET_STRING_TIMESERIES= "Timeseries";
	private static final String GET_STRING_STORAGE_GROUP= "Storage Group";
	private static final String GET_STRING_DATATYPE= "DataType";
	private static final String GET_STRING_ENCODING= "Encoding";


	/**
	 * Constructor used for COLUMN or DELTA_OBJECT or SHOW_STORAGE_GROUP results
	 *
	 * @param columnSchemas
	 * @param deltaObjectList
	 * @param storageGroupSet
	 */
	public TsfileMetadataResultSet(List<ColumnSchema> columnSchemas, List<String> deltaObjectList, Set<String> storageGroupSet) {
		if (columnSchemas != null) { // 'showLabels' is not needed in this type
			type = MetadataType.COLUMN;
			columnItr = columnSchemas.iterator();
		} else if (deltaObjectList != null || storageGroupSet != null) {
			type = MetadataType.DELTA_OBJECT_OR_STORAGE_GROUP;
			colCount = 1;
			maxValueLength = new int[1]; // one fixed column
			if (deltaObjectList != null) {
				showLabels = new String[]{"Device"};
				columnItr = deltaObjectList.iterator();
				int tmp = showLabels[0].length();
				maxValueLength[0] = getMaxValueLength(deltaObjectList, tmp);
			} else {
				showLabels = new String[]{"Storage Group"};
				columnItr = storageGroupSet.iterator();
				int tmp = showLabels[0].length();
				maxValueLength[0] = getMaxValueLength(new ArrayList<String>(storageGroupSet), tmp);
			}
		}
	}

	/**
	 * Constructor used for SHOW_TIMESERIES_PATH results
	 *
	 * @param tslist
	 */
	public TsfileMetadataResultSet(List<List<String>> tslist) {
		type = MetadataType.SHOW_TIMESERIES;
		showLabels = new String[]{"Timeseries", "Storage Group", "DataType", "Encoding"};
		colCount = 4;
		columnItr = tslist.iterator();
		maxValueLength = new int[colCount];
		for (int i = 0; i < colCount; i++) { // get the max value length of each column for table display
			int tmp = showLabels[i].length();
			for (List<String> tsrow : tslist) {
				int len = tsrow.get(i).length();
				tmp = tmp > len ? tmp : len;
			}
			maxValueLength[i] = tmp;
		}
	}

	/**
	 * @param ObjectList
	 * @param initLength
	 * @return the max string length of a column
	 */
	private int getMaxValueLength(List<String> ObjectList, int initLength) {
		for (String Object : ObjectList) {
			int len = Object.length();
			initLength = initLength > len ? initLength : len;
		}
		return initLength;
	}

	private void checkType() throws SQLException {
		if (type == MetadataType.COLUMN) {
			throw new SQLException("Method not supported");
		}
	}

	public int getMaxValueLength(int columnIndex) throws SQLException { // start from 1
		checkType();

		if (columnIndex >= 1 && columnIndex <= colCount) {
			return maxValueLength[columnIndex - 1];
		} else if (columnIndex > colCount) {
			throw new SQLException(String.format("select column index %d does not exists", columnIndex));
		} else { // columnIndex <= 0
			throw new SQLException("column index should start from 1");
		}
	}

	public int getColCount() throws SQLException {
		checkType();
		return colCount;
	}

	public String[] getShowLabels() throws SQLException {
		checkType();
		return showLabels;
	}


	@Override
	public void close() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getConcurrency() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getFetchDirection() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if (type == MetadataType.COLUMN) {
			throw new SQLException("Method not supported for COLUMN type");
		}
		return new TsfileMetadataResultMetadata(showLabels);

	}

	@Override
	public boolean next() throws SQLException {
		boolean hasNext = columnItr.hasNext();
		switch (type) {
			case COLUMN:
				if (hasNext) {
					currentColumn = (ColumnSchema) columnItr.next();
				}
				return hasNext;
			case DELTA_OBJECT_OR_STORAGE_GROUP:
				if (hasNext) {
					currentDeltaObjectOrStorageGroup = (String) columnItr.next();
				}
				return hasNext;
			case SHOW_TIMESERIES:
				if (hasNext) {
					currentTimeseries = (List<String>) (columnItr.next());
				}
				return hasNext;
			default:
				break;
		}
		return false;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		switch (type) {
			case DELTA_OBJECT_OR_STORAGE_GROUP:
				if (columnIndex == 1) {
					return getString(GET_STRING_DELTA_OBJECT_OR_STORAGE_GROUP);
				}
				break;
			case COLUMN:
				if (columnIndex == 1) {
					return getString(GET_STRING_COLUMN_NAME);
				} else if (columnIndex == 2) {
					return getString(GET_STRING_COLUMN_TYPE);
				}
				break;
			case SHOW_TIMESERIES:
				if (columnIndex >= 1 && columnIndex <= colCount) {
					return getString(showLabels[columnIndex - 1]);
				}
				break;
			default:
				break;
		}
		throw new SQLException(String.format("select column index %d does not exists", columnIndex));
	}

	@Override
	public String getString(String columnName) throws SQLException {
		// use special key word to judge return content
		switch (columnName) {
			case GET_STRING_COLUMN_NAME:
				return currentColumn.name;
			case GET_STRING_COLUMN_TYPE:
				if (currentColumn.dataType != null) {
					return currentColumn.dataType.toString();
				}

			case GET_STRING_DELTA_OBJECT_OR_STORAGE_GROUP:
				return currentDeltaObjectOrStorageGroup;

			case GET_STRING_TIMESERIES:
				return currentTimeseries.get(0);
			case GET_STRING_STORAGE_GROUP:
				return currentTimeseries.get(1);
			case GET_STRING_DATATYPE:
				return currentTimeseries.get(2);
			case GET_STRING_ENCODING:
				return currentTimeseries.get(3);

			default:
				break;
		}
		return null;
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public int getType() throws SQLException {
		return type.ordinal();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isClosed() throws SQLException {
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLException("Method not supported");
	}

	private enum MetadataType{
		DELTA_OBJECT_OR_STORAGE_GROUP, COLUMN, SHOW_TIMESERIES
	}
}
