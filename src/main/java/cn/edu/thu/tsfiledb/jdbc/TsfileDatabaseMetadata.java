package cn.edu.thu.tsfiledb.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchMetadataReq;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSFetchMetadataResp;
import cn.edu.thu.tsfiledb.service.rpc.thrift.TSIService;

public class TsfileDatabaseMetadata implements DatabaseMetaData {
//	private final String ROOT_PATH = "root";
	private TsfileConnection connection;
	private TSIService.Iface client;

	public TsfileDatabaseMetadata(TsfileConnection connection, TSIService.Iface client) {
		this.connection = connection;
		this.client = client;
	}

	/**
	 * if deltaObjectPattern != null, return all delta object
	 * if deltaObjectPattern == null and columnPattern != null, return column schema， otherwise return null
	 */
	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String columnPattern, String deltaObjectPattern)
			throws SQLException {
		try {
			return getColumnsOrDeltaObject(catalog, schemaPattern, columnPattern, deltaObjectPattern);
		} catch (TException e) {
			boolean flag = connection.reconnect();
			this.client = connection.client;
			if (flag) {
				try {
					return getColumnsOrDeltaObject(catalog, schemaPattern, columnPattern, deltaObjectPattern);
				} catch (TException e2) {
					throw new SQLException(String.format(
							"Fail to get colums catalog=%s, schemaPattern=%s,"
									+ " columnPattern=%s, deltaObjectPattern=%s after reconnecting. please check server status",
							catalog, schemaPattern, columnPattern, deltaObjectPattern));
				}
			} else {
				throw new SQLException(String.format(
						"Fail to reconnect to server when getting colums catalog=%s, schemaPattern=%s,"
								+ " columnPattern=%s, deltaObjectPattern=%s after reconnecting. please check server status",
						catalog, schemaPattern, columnPattern, deltaObjectPattern));
			}
		}
	}
	
	private ResultSet getColumnsOrDeltaObject(String catalog, String schemaPattern, String columnPattern, String deltaObjectPattern) throws TException, SQLException{
	    if(deltaObjectPattern != null && !deltaObjectPattern.trim().equals("")){
			TSFetchMetadataReq req = new TSFetchMetadataReq("DELTA_OBEJECT");
			TSFetchMetadataResp resp;
			try {
				resp = client.fetchMetadata(req);
				Utils.verifySuccess(resp.getStatus());
				Map<String, List<String>> deltaObjectList = resp.getDeltaObjectMap();
				if(deltaObjectList == null || !deltaObjectList.containsKey(deltaObjectPattern)){
					new TsfileMetadataResultSet(null, new ArrayList<>());
				}
				return new TsfileMetadataResultSet(null, deltaObjectList.get(deltaObjectPattern));
			} catch (TException e) {
				throw new TException("Conncetion error when fetching delta object metadata", e);
			}
			
		}

		if(columnPattern != null && !columnPattern.trim().equals("")){
		    	TSFetchMetadataReq req;
		    	if(!columnPattern.endsWith("*")){
		    	    	req = new TSFetchMetadataReq("COLUMN"); 
		    	    	req.setColumnPath(columnPattern);
				try {
				    	TSFetchMetadataResp resp = client.fetchMetadata(req);
					Utils.verifySuccess(resp.getStatus());
					List<ColumnSchema> columnSchemaNew = new ArrayList<>();
					if(resp.getDataType() != null){
						columnSchemaNew.add(new ColumnSchema(columnPattern, 
								TSDataType.valueOf(resp.getDataType()), 
								null));
					}
					return new TsfileMetadataResultSet(columnSchemaNew, null);
				} catch (TException e) {
					throw new TException("Conncetion error when fetching column data type", e);
				}
		    	} else{
		    	    	req = new TSFetchMetadataReq("ALL_COLUMNS");
		    	    	req.setColumnPath(columnPattern);
				try {
				    	TSFetchMetadataResp resp = client.fetchMetadata(req);
					Utils.verifySuccess(resp.getStatus());
					List<ColumnSchema> columnSchemaNew = new ArrayList<>();
					if(resp.getAllColumns() != null){
					    for(String path : resp.getAllColumns()){
						columnSchemaNew.add(new ColumnSchema(path, null, null));
					    }
					}
					return new TsfileMetadataResultSet(columnSchemaNew, null);
				} catch (TException e) {
					throw new TException("Conncetion error when fetching column data type", e);
				}
		    	}
		}
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean deletesAreDetected(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "TsFileDB";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDriverMajorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDriverMinorVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getDriverName() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getDriverVersion() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public int getSQLStateType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getStringFunctions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String getURL() throws SQLException {
		// TODO: Return the URL for this DBMS or null if it cannot be generated
		return null;
	}

	@Override
	public String getUserName() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		// TODO Auto-generated method stub
		throw new SQLException("Method not supported");
	}

	@Override
	public String toString() {
		try {
			return getFullTimeseries();
		} catch (TException e) {
			boolean flag = connection.reconnect();
			this.client = connection.client;
			if (flag) {
				try {
					return getFullTimeseries();
				} catch (TException e2) {
					System.out.println("Fail to get all timeseries "
							+ "info after reconnecting. please check server status");
				} catch (TsfileSQLException e1) {}
			} else {
				System.out.println("Fail to reconnect to server "
						+ "when getting all timeseries info. please check server status");
			}
		} catch (TsfileSQLException e) {}
		return null;
	}
	
	private String getFullTimeseries() throws TException, TsfileSQLException{
		TSFetchMetadataReq req = new TSFetchMetadataReq("METADATA_IN_JSON");
		TSFetchMetadataResp resp;
		resp = client.fetchMetadata(req);
		Utils.verifySuccess(resp.getStatus());
		return resp.getMetadataInJson();
	}
}
