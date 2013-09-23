/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.lingual.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import cascading.flow.planner.PlatformInfo;
import cascading.lingual.platform.PlatformBroker;
import cascading.lingual.util.Version;
import net.hydromatic.optiq.jdbc.Helper;

/**
 *
 */
class LingualDatabaseMetaData implements DatabaseMetaData
  {
  private final LingualConnection connection;
  private final DatabaseMetaData parent;

  public LingualDatabaseMetaData( LingualConnection connection, DatabaseMetaData parent )
    {
    this.connection = connection;
    this.parent = parent;
    }

  @Override
  public boolean allProceduresAreCallable() throws SQLException
    {
    return false;
    }

  @Override
  public boolean allTablesAreSelectable() throws SQLException
    {
    return true;
    }

  @Override
  public String getURL() throws SQLException
    {
    // optiq doesn't support this so get it from the connection.
    return connection.getMetaData().getURL();
    }

  @Override
  public String getUserName() throws SQLException
    {
    return System.getProperty( "user.name" );
    }

  @Override
  public boolean isReadOnly() throws SQLException
    {
    return false;
    }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException
    {
    return false;
    }

  @Override
  public boolean nullsAreSortedLow() throws SQLException
    {
    return false;
    }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException
    {
    return false;
    }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException
    {
    return false;
    }

  @Override
  public String getDatabaseProductName() throws SQLException
    {
    PlatformBroker platformBroker = connection.getPlatformBroker();
    PlatformInfo platformInfo = platformBroker.getPlatformInfo();

    return String.format( "%s [%s %s]",
      Version.getProductName(),
      platformInfo.name,
      platformInfo.vendor
    );
    }

  @Override
  public String getDatabaseProductVersion() throws SQLException
    {
    PlatformBroker platformBroker = connection.getPlatformBroker();
    PlatformInfo platformInfo = platformBroker.getPlatformInfo();

    return String.format( "%s [%s]",
      Version.getProductVersion(),
      platformInfo.version
    );
    }

  @Override
  public String getDriverName() throws SQLException
    {
    return parent.getDriverName();
    }

  @Override
  public String getDriverVersion() throws SQLException
    {
    return parent.getDriverVersion();
    }

  @Override
  public int getDriverMajorVersion()
    {
    return parent.getDriverMajorVersion();
    }

  @Override
  public int getDriverMinorVersion()
    {
    return parent.getDriverMinorVersion();
    }

  @Override
  public boolean usesLocalFiles() throws SQLException
    {
    return parent.usesLocalFiles();
    }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException
    {
    return parent.usesLocalFilePerTable();
    }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
    return parent.supportsMixedCaseIdentifiers();
    }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException
    {
    return parent.storesUpperCaseIdentifiers();
    }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException
    {
    return parent.storesLowerCaseIdentifiers();
    }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException
    {
    return parent.storesMixedCaseIdentifiers();
    }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
    return parent.supportsMixedCaseQuotedIdentifiers();
    }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
    return parent.storesUpperCaseQuotedIdentifiers();
    }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
    return parent.storesLowerCaseQuotedIdentifiers();
    }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
    return parent.storesMixedCaseQuotedIdentifiers();
    }

  @Override
  public String getIdentifierQuoteString() throws SQLException
    {
    return parent.getIdentifierQuoteString();
    }

  @Override
  public String getSQLKeywords() throws SQLException
    {
    return "";
    }

  @Override
  public String getNumericFunctions() throws SQLException
    {
    return "";
    }

  @Override
  public String getStringFunctions() throws SQLException
    {
    return "";
    }

  @Override
  public String getSystemFunctions() throws SQLException
    {
    return parent.getSystemFunctions();
    }

  @Override
  public String getTimeDateFunctions() throws SQLException
    {
    return "";
    }

  @Override
  public String getSearchStringEscape() throws SQLException
    {
    return parent.getSearchStringEscape();
    }

  @Override
  public String getExtraNameCharacters() throws SQLException
    {
    return parent.getExtraNameCharacters();
    }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
    return parent.supportsAlterTableWithAddColumn();
    }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
    return parent.supportsAlterTableWithDropColumn();
    }

  @Override
  public boolean supportsColumnAliasing() throws SQLException
    {
    return parent.supportsColumnAliasing();
    }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException
    {
    return parent.nullPlusNonNullIsNull();
    }

  @Override
  public boolean supportsConvert() throws SQLException
    {
    return parent.supportsConvert();
    }

  @Override
  public boolean supportsConvert( int fromType, int toType ) throws SQLException
    {
    return parent.supportsConvert( fromType, toType );
    }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException
    {
    return parent.supportsTableCorrelationNames();
    }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
    return parent.supportsDifferentTableCorrelationNames();
    }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException
    {
    return parent.supportsExpressionsInOrderBy();
    }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException
    {
    return parent.supportsOrderByUnrelated();
    }

  @Override
  public boolean supportsGroupBy() throws SQLException
    {
    return parent.supportsGroupBy();
    }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException
    {
    return parent.supportsGroupByUnrelated();
    }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException
    {
    return parent.supportsGroupByBeyondSelect();
    }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException
    {
    return parent.supportsLikeEscapeClause();
    }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException
    {
    return parent.supportsNonNullableColumns();
    }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException
    {
    return parent.supportsMinimumSQLGrammar();
    }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException
    {
    return parent.supportsCoreSQLGrammar();
    }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException
    {
    return parent.supportsExtendedSQLGrammar();
    }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
    return parent.supportsANSI92EntryLevelSQL();
    }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
    return parent.supportsANSI92IntermediateSQL();
    }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException
    {
    return parent.supportsANSI92FullSQL();
    }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
    return parent.supportsIntegrityEnhancementFacility();
    }

  @Override
  public boolean supportsOuterJoins() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException
    {
    return true;
    }

  @Override
  public String getSchemaTerm() throws SQLException
    {
    return "schema";
    }

  @Override
  public String getProcedureTerm() throws SQLException
    {
    return "procedure";
    }

  @Override
  public String getCatalogTerm() throws SQLException
    {
    return "catalog";
    }

  @Override
  public boolean isCatalogAtStart() throws SQLException
    {
    return true;
    }

  @Override
  public String getCatalogSeparator() throws SQLException
    {
    return ".";
    }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
    return parent.supportsSchemasInProcedureCalls();
    }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
    return parent.supportsSchemasInIndexDefinitions();
    }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
    return parent.supportsSchemasInPrivilegeDefinitions();
    }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
    return parent.supportsCatalogsInIndexDefinitions();
    }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
    return parent.supportsCatalogsInPrivilegeDefinitions();
    }

  @Override
  public boolean supportsPositionedDelete() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsStoredProcedures() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsUnion() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsUnionAll() throws SQLException
    {
    return true;
    }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
    return parent.supportsOpenCursorsAcrossCommit();
    }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
    return parent.supportsOpenCursorsAcrossRollback();
    }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
    return parent.supportsOpenStatementsAcrossCommit();
    }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
    return parent.supportsOpenStatementsAcrossRollback();
    }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException
    {
    return parent.getMaxBinaryLiteralLength();
    }

  @Override
  public int getMaxCharLiteralLength() throws SQLException
    {
    return parent.getMaxCharLiteralLength();
    }

  @Override
  public int getMaxColumnNameLength() throws SQLException
    {
    return parent.getMaxColumnNameLength();
    }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException
    {
    return parent.getMaxColumnsInGroupBy();
    }

  @Override
  public int getMaxColumnsInIndex() throws SQLException
    {
    return parent.getMaxColumnsInIndex();
    }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException
    {
    return parent.getMaxColumnsInOrderBy();
    }

  @Override
  public int getMaxColumnsInSelect() throws SQLException
    {
    return parent.getMaxColumnsInSelect();
    }

  @Override
  public int getMaxColumnsInTable() throws SQLException
    {
    return parent.getMaxColumnsInTable();
    }

  @Override
  public int getMaxConnections() throws SQLException
    {
    return parent.getMaxConnections();
    }

  @Override
  public int getMaxCursorNameLength() throws SQLException
    {
    return parent.getMaxCursorNameLength();
    }

  @Override
  public int getMaxIndexLength() throws SQLException
    {
    return parent.getMaxIndexLength();
    }

  @Override
  public int getMaxSchemaNameLength() throws SQLException
    {
    return parent.getMaxSchemaNameLength();
    }

  @Override
  public int getMaxProcedureNameLength() throws SQLException
    {
    return parent.getMaxProcedureNameLength();
    }

  @Override
  public int getMaxCatalogNameLength() throws SQLException
    {
    return parent.getMaxCatalogNameLength();
    }

  @Override
  public int getMaxRowSize() throws SQLException
    {
    return parent.getMaxRowSize();
    }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
    {
    return parent.doesMaxRowSizeIncludeBlobs();
    }

  @Override
  public int getMaxStatementLength() throws SQLException
    {
    return parent.getMaxStatementLength();
    }

  @Override
  public int getMaxStatements() throws SQLException
    {
    return parent.getMaxStatements();
    }

  @Override
  public int getMaxTableNameLength() throws SQLException
    {
    return parent.getMaxTableNameLength();
    }

  @Override
  public int getMaxTablesInSelect() throws SQLException
    {
    return parent.getMaxTablesInSelect();
    }

  @Override
  public int getMaxUserNameLength() throws SQLException
    {
    return parent.getMaxUserNameLength();
    }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException
    {
    return Connection.TRANSACTION_NONE;
    }

  @Override
  public boolean supportsTransactions() throws SQLException
    {
    return parent.supportsTransactions();
    }

  @Override
  public boolean supportsTransactionIsolationLevel( int level ) throws SQLException
    {
    return parent.supportsTransactionIsolationLevel( level );
    }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    {
    return parent.supportsDataDefinitionAndDataManipulationTransactions();
    }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException
    {
    return parent.supportsDataManipulationTransactionsOnly();
    }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException
    {
    return parent.dataDefinitionCausesTransactionCommit();
    }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
    return parent.dataDefinitionIgnoredInTransactions();
    }

  @Override
  public ResultSet getProcedures( String catalog, String schemaPattern, String procedureNamePattern ) throws SQLException
    {
    return parent.getProcedures( catalog, schemaPattern, procedureNamePattern );
    }

  @Override
  public ResultSet getProcedureColumns( String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern ) throws SQLException
    {
    return parent.getProcedureColumns( catalog, schemaPattern, procedureNamePattern, columnNamePattern );
    }

  @Override
  public ResultSet getTables( String catalog, String schemaPattern, String tableNamePattern, String[] types ) throws SQLException
    {
    return parent.getTables( catalog, schemaPattern, tableNamePattern, types );
    }

  @Override
  public ResultSet getSchemas() throws SQLException
    {
    return parent.getSchemas();
    }

  @Override
  public ResultSet getCatalogs() throws SQLException
    {
    return parent.getCatalogs();
    }

  @Override
  public ResultSet getTableTypes() throws SQLException
    {
    return parent.getTableTypes();
    }

  @Override
  public ResultSet getColumns( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern ) throws SQLException
    {
    return parent.getColumns( catalog, schemaPattern, tableNamePattern, columnNamePattern );
    }

  @Override
  public ResultSet getColumnPrivileges( String catalog, String schema, String table, String columnNamePattern ) throws SQLException
    {
    return parent.getColumnPrivileges( catalog, schema, table, columnNamePattern );
    }

  @Override
  public ResultSet getTablePrivileges( String catalog, String schemaPattern, String tableNamePattern ) throws SQLException
    {
    return parent.getTablePrivileges( catalog, schemaPattern, tableNamePattern );
    }

  @Override
  public ResultSet getBestRowIdentifier( String catalog, String schema, String table, int scope, boolean nullable ) throws SQLException
    {
    return parent.getBestRowIdentifier( catalog, schema, table, scope, nullable );
    }

  @Override
  public ResultSet getVersionColumns( String catalog, String schema, String table ) throws SQLException
    {
    return parent.getVersionColumns( catalog, schema, table );
    }

  @Override
  public ResultSet getPrimaryKeys( String catalog, String schema, String table ) throws SQLException
    {
    return parent.getPrimaryKeys( catalog, schema, table );
    }

  @Override
  public ResultSet getImportedKeys( String catalog, String schema, String table ) throws SQLException
    {
    return parent.getImportedKeys( catalog, schema, table );
    }

  @Override
  public ResultSet getExportedKeys( String catalog, String schema, String table ) throws SQLException
    {
    return parent.getExportedKeys( catalog, schema, table );
    }

  @Override
  public ResultSet getCrossReference( String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) throws SQLException
    {
    return parent.getCrossReference( parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
    }

  @Override
  public ResultSet getTypeInfo() throws SQLException
    {
    return parent.getTypeInfo();
    }

  @Override
  public ResultSet getIndexInfo( String catalog, String schema, String table, boolean unique, boolean approximate ) throws SQLException
    {
    return parent.getIndexInfo( catalog, schema, table, unique, approximate );
    }

  @Override
  public boolean supportsResultSetType( int type ) throws SQLException
    {
    return parent.supportsResultSetType( type );
    }

  @Override
  public boolean supportsResultSetConcurrency( int type, int concurrency ) throws SQLException
    {
    return parent.supportsResultSetConcurrency( type, concurrency );
    }

  @Override
  public boolean ownUpdatesAreVisible( int type ) throws SQLException
    {
    return parent.ownUpdatesAreVisible( type );
    }

  @Override
  public boolean ownDeletesAreVisible( int type ) throws SQLException
    {
    return parent.ownDeletesAreVisible( type );
    }

  @Override
  public boolean ownInsertsAreVisible( int type ) throws SQLException
    {
    return parent.ownInsertsAreVisible( type );
    }

  @Override
  public boolean othersUpdatesAreVisible( int type ) throws SQLException
    {
    return parent.othersUpdatesAreVisible( type );
    }

  @Override
  public boolean othersDeletesAreVisible( int type ) throws SQLException
    {
    return parent.othersDeletesAreVisible( type );
    }

  @Override
  public boolean othersInsertsAreVisible( int type ) throws SQLException
    {
    return parent.othersInsertsAreVisible( type );
    }

  @Override
  public boolean updatesAreDetected( int type ) throws SQLException
    {
    return parent.updatesAreDetected( type );
    }

  @Override
  public boolean deletesAreDetected( int type ) throws SQLException
    {
    return parent.deletesAreDetected( type );
    }

  @Override
  public boolean insertsAreDetected( int type ) throws SQLException
    {
    return parent.insertsAreDetected( type );
    }

  @Override
  public boolean supportsBatchUpdates() throws SQLException
    {
    return parent.supportsBatchUpdates();
    }

  @Override
  public ResultSet getUDTs( String catalog, String schemaPattern, String typeNamePattern, int[] types ) throws SQLException
    {
    return parent.getUDTs( catalog, schemaPattern, typeNamePattern, types );
    }

  @Override
  public Connection getConnection() throws SQLException
    {
    return parent.getConnection();
    }

  @Override
  public boolean supportsSavepoints() throws SQLException
    {
    return false;
    }

  @Override
  public boolean supportsNamedParameters() throws SQLException
    {
    return parent.supportsNamedParameters();
    }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException
    {
    return parent.supportsMultipleOpenResults();
    }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException
    {
    return parent.supportsGetGeneratedKeys();
    }

  @Override
  public ResultSet getSuperTypes( String catalog, String schemaPattern, String typeNamePattern ) throws SQLException
    {
    return parent.getSuperTypes( catalog, schemaPattern, typeNamePattern );
    }

  @Override
  public ResultSet getSuperTables( String catalog, String schemaPattern, String tableNamePattern ) throws SQLException
    {
    return parent.getSuperTables( catalog, schemaPattern, tableNamePattern );
    }

  @Override
  public ResultSet getAttributes( String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern ) throws SQLException
    {
    return parent.getAttributes( catalog, schemaPattern, typeNamePattern, attributeNamePattern );
    }

  @Override
  public boolean supportsResultSetHoldability( int holdability ) throws SQLException
    {
    return parent.supportsResultSetHoldability( holdability );
    }

  @Override
  public int getResultSetHoldability() throws SQLException
    {
    return parent.getResultSetHoldability();
    }

  @Override
  public int getDatabaseMajorVersion() throws SQLException
    {
    return parent.getDatabaseMajorVersion();
    }

  @Override
  public int getDatabaseMinorVersion() throws SQLException
    {
    return parent.getDatabaseMinorVersion();
    }

  @Override
  public int getJDBCMajorVersion() throws SQLException
    {
    return parent.getJDBCMajorVersion();
    }

  @Override
  public int getJDBCMinorVersion() throws SQLException
    {
    return parent.getJDBCMinorVersion();
    }

  @Override
  public int getSQLStateType() throws SQLException
    {
    return parent.getSQLStateType();
    }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException
    {
    return parent.locatorsUpdateCopy();
    }

  @Override
  public boolean supportsStatementPooling() throws SQLException
    {
    return parent.supportsStatementPooling();
    }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException
    {
    return parent.getRowIdLifetime();
    }

  @Override
  public ResultSet getSchemas( String catalog, String schemaPattern ) throws SQLException
    {
    return parent.getSchemas( catalog, schemaPattern );
    }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException
    {
    return parent.supportsStoredFunctionsUsingCallSyntax();
    }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException
    {
    return parent.autoCommitFailureClosesAllResultSets();
    }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException
    {
    return parent.getClientInfoProperties();
    }

  @Override
  public ResultSet getFunctions( String catalog, String schemaPattern, String functionNamePattern ) throws SQLException
    {
    return parent.getFunctions( catalog, schemaPattern, functionNamePattern );
    }

  @Override
  public ResultSet getFunctionColumns( String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern ) throws SQLException
    {
    return parent.getFunctionColumns( catalog, schemaPattern, functionNamePattern, columnNamePattern );
    }

  public ResultSet getPseudoColumns( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern ) throws SQLException
    {
    return Helper.INSTANCE.createEmptyResultSet( connection.getParent() );
    }

  public boolean generatedKeyAlwaysReturned() throws SQLException
    {
    // Dummied out to allow compiling against JDK 1.7
    // can't cast this to OptiqDatabaseMetaData since class is private. As of Optiq 0.3.3 even if we could cast it the method just throws an exception anyway
    throw new UnsupportedOperationException( "JDBC feature for JDK 1.7 not yet supported." );
    }

  @Override
  public <T> T unwrap( Class<T> iface ) throws SQLException
    {
    return parent.unwrap( iface );
    }

  @Override
  public boolean isWrapperFor( Class<?> iface ) throws SQLException
    {
    return parent.isWrapperFor( iface );
    }
  }
