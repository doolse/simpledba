package io.doolse.simpledba.postgres

import java.sql.SQLType
import java.util.UUID

import io.doolse.simpledba.Iso
import io.doolse.simpledba.jdbc.{StdJDBCColumn, WrappedColumn}

case class PostgresColumn[AA](wrapped: StdJDBCColumn[AA]) extends WrappedColumn[AA, PostgresColumn]
{
  override def mapped[B] = PostgresColumn.apply[B]
}

case object JSONBType extends SQLType {
  def getName = "JSONB"

  def getVendorTypeNumber = 1

  def getVendor = "io.doolse.simpledba.postgres"
}

object PostgresColumn
{
  implicit def stdCol[A](implicit std: StdJDBCColumn[A]) = PostgresColumn(std)

  implicit def isoCol[A, B](implicit iso: Iso[B, A], pgCol: PostgresColumn[A]): PostgresColumn[B]
  = pgCol.isoMap(iso)

  implicit def uuidCol = PostgresColumn[UUID](StdJDBCColumn.uuidCol)

  def jsonbColumn = PostgresColumn(StdJDBCColumn.stringCol.copy(cast = c => s"$c::JSONB"))
}