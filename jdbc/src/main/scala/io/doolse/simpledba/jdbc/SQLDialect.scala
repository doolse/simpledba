package io.doolse.simpledba.jdbc

trait SQLDialect {
  def querySQL(query: JDBCPreparedQuery): String
  def escapeTableName(table: String): String
  def escapeColumnName(column: String): String
  def expressionSQL(expression: SQLExpression): String
  def dropTable(t: TableDefinition): String
  def createTable(t: TableDefinition): String
  def addColumns(t: TableColumns): Seq[String]
  def truncateTable(t: TableDefinition): String
  def createIndex(t: TableColumns, named: String): String
}
