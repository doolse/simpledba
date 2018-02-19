package io.doolse.simpledba.jdbc

import java.sql.{Array => _, _}
import java.time.Instant
import java.util.UUID
import javax.xml.transform.dom.{DOMResult, DOMSource}

import io.doolse.simpledba.{IsoAtom, PartialIsoAtom}
import org.w3c.dom.Node

import scala.collection.mutable

/**
  * Created by jolz on 12/03/17.
  */
case class JDBCColumn[A](columnType: SQLType, byName: (JDBCSession, ResultSet, String) => Option[A],
  byIndex: (JDBCSession, ResultSet, Int) => Option[A],
  bind: (JDBCSession, PreparedStatement, Int, A) => Unit,
  bindingObject: (JDBCSession, A) => AnyRef)

object JDBCColumn {
  case object UuidSQLType extends SQLType {
    def getName = "UUID"
    def getVendorTypeNumber = 1
    def getVendor = "io.doolse.simpledba"
  }

  case class ArraySQLType(elementType: SQLType) extends SQLType {
    def getName = "ARRAY"
    def getVendorTypeNumber = 2
    def getVendor = "io.doolse.simpledba"
  }

  case class SizedSQLType(subType: SQLType, size: Int) extends SQLType {
    def getName = "SIZED"
    def getVendorTypeNumber = 3
    def getVendor = "io.doolse.simpledba"
  }

  def dialectColumn[A](columnType: SQLType) : JDBCColumn[A] = JDBCColumn[A](columnType,
    (s: JDBCSession, r: ResultSet, n: String) => s.config.specialType(columnType).byName(r, n).asInstanceOf[Option[A]],
    (s:JDBCSession, rs: ResultSet, i: Int) => s.config.specialType(columnType).byIndex(rs, i).asInstanceOf[Option[A]],
    (s: JDBCSession, ps: PreparedStatement, i: Int, a: A) => s.config.specialType(columnType).bind(ps, i, a.asInstanceOf[AnyRef]),
    (s, a) => s.config.specialType(columnType).bindingObject(a.asInstanceOf[AnyRef])
  )

  def direct[A](columnType: SQLType,
                byName: ResultSet => String => A,
                byIndex: ResultSet => Int => A,
                bindIndex: (PreparedStatement,Int,A) => Unit,
                bindingObj: A => AnyRef) : JDBCColumn[A] =
    JDBCColumn[A](columnType, (s:JDBCSession,rs:ResultSet,n:String) => Option(byName(rs)(n)).filterNot(_ => rs.wasNull),
      (s:JDBCSession,rs:ResultSet,i:Int) => Option(byIndex(rs)(i)).filterNot(_ => rs.wasNull),
      (s:JDBCSession,ps:PreparedStatement,i:Int,a:A) => bindIndex(ps,i,a),
      (s:JDBCSession, a:A) => bindingObj(a))

  def wrappedColumn[S, A](wrapped: JDBCColumn[A], to: S => A, from: A => S) : JDBCColumn[S] = JDBCColumn[S](wrapped.columnType,
    (s:JDBCSession,rs:ResultSet,n:String) => wrapped.byName(s,rs,n).map(from),
    (s:JDBCSession,rs:ResultSet,i:Int) => wrapped.byIndex(s,rs,i).map(from),
    (s:JDBCSession,ps:PreparedStatement,i:Int,a:S) => wrapped.bind(s,ps,i,to(a)),
    (s,a) => wrapped.bindingObject(s, to(a))
  )

  implicit val uuidColumn : JDBCColumn[UUID] = dialectColumn[UUID](UuidSQLType)
  implicit val stringColumn : JDBCColumn[String] = direct(JDBCType.LONGNVARCHAR, _.getString, _.getString, (ps,i,a) => ps.setString(i, a), identity)
  implicit val intColumn : JDBCColumn[Int] = direct(JDBCType.INTEGER, _.getInt, _.getInt, (ps,i,a) => ps.setInt(i, a), _.asInstanceOf[java.lang.Integer])
  implicit val longColumn : JDBCColumn[Long] = direct(JDBCType.BIGINT, _.getLong, _.getLong, (ps,i,a) => ps.setLong(i, a), _.asInstanceOf[java.lang.Long])
  implicit val boolColumn : JDBCColumn[Boolean] = direct(JDBCType.BOOLEAN, _.getBoolean, _.getBoolean, (ps,i,a) => ps.setBoolean(i, a), _.asInstanceOf[java.lang.Boolean])
  implicit val shortColumn : JDBCColumn[Short] = direct(JDBCType.SMALLINT, _.getShort, _.getShort, (ps,i,a) => ps.setShort(i, a), _.asInstanceOf[java.lang.Short])
  implicit val floatColumn : JDBCColumn[Float] = direct(JDBCType.FLOAT, _.getFloat, _.getFloat, (ps,i,a) => ps.setFloat(i, a), _.asInstanceOf[java.lang.Float])
  implicit val doubleColumn : JDBCColumn[Double] = direct(JDBCType.DOUBLE, _.getDouble, _.getDouble, (ps,i,a) => ps.setDouble(i, a), _.asInstanceOf[java.lang.Double])
  val timestampColumn : JDBCColumn[Timestamp] = direct(JDBCType.TIMESTAMP, _.getTimestamp, _.getTimestamp, (ps,i,a) => ps.setTimestamp(i, a), identity)
  val sqlArrayColumn : JDBCColumn[java.sql.Array] = direct(JDBCType.ARRAY, _.getArray, _.getArray, (ps,i,a) => ps.setArray(i, a), identity)

  implicit val instantColumn : JDBCColumn[Instant] = wrappedColumn[Instant, Timestamp](timestampColumn, i => new Timestamp(i.toEpochMilli), t => Instant.ofEpochMilli(t.getTime))

  implicit def isoColumn[A, B](implicit iso: PartialIsoAtom[A, B], oc: JDBCColumn[B]): JDBCColumn[A] = wrappedColumn[A, B](oc, iso.from, iso.to)

  implicit def optionalCol[A](implicit col: JDBCColumn[A]): JDBCColumn[Option[A]] = JDBCColumn[Option[A]](col.columnType,
    (c: JDBCSession, r: ResultSet, n: String) => Some(col.byName(c, r, n)),
    (c: JDBCSession, r: ResultSet, i: Int) => Some(col.byIndex(c, r, i)),
    (c: JDBCSession, ps: PreparedStatement, i: Int, oa: Option[A]) => oa.fold(ps.setNull(i, java.sql.Types.NULL))((a:A) => col.bind(c, ps, i, a)),
    (s,a) => a.map(aa => col.bindingObject(s, aa)).orNull
  )

  implicit def arrayCol[A](implicit col: JDBCColumn[A]): JDBCColumn[Vector[A]] = {
    def convertArray(s: JDBCSession)(a: java.sql.Array): Vector[A] = {
      val ars = a.getResultSet
      val b = mutable.Buffer[A]()
      while (ars.next()) {
        col.byIndex.apply(s, ars, 2).foreach(b +=)
      }
      b.toVector
    }
    val wrappedCol = col.columnType
    def createBinding(s: JDBCSession, a: Vector[A]) =
      s.connection.createArrayOf(s.config.sqlTypeToString(wrappedCol), a.map(col.bindingObject(s, _)).toArray)
    JDBCColumn[Vector[A]](ArraySQLType(wrappedCol),
      (s: JDBCSession, rs: ResultSet, n: String) => sqlArrayColumn.byName.apply(s, rs, n).map(convertArray(s)),
      (s:JDBCSession, rs:ResultSet, i: Int) => sqlArrayColumn.byIndex.apply(s, rs, i).map(convertArray(s)),
      (s:JDBCSession, ps:PreparedStatement, i:Int, v:Vector[A]) => sqlArrayColumn.bind(s, ps, i, createBinding(s, v)),
      createBinding
      )
  }

  implicit val xmlColumn : JDBCColumn[Node] = {
    def asNode(sql: SQLXML): Option[Node] = {
      Option(sql).map {
        sn =>
          val n = sn.getSource(classOf[DOMSource]).getNode()
          sn.free()
          n
      }
    }

    def bindingObject(s: JDBCSession, a: Node) = {
      val sqlXml = s.connection.createSQLXML()
      sqlXml.setResult(classOf[DOMResult]).setNode(a)
      sqlXml
    }

    JDBCColumn[Node](JDBCType.SQLXML,
      (s, rs, n) => asNode(rs.getSQLXML(n)),
      (s, rs, i) => asNode(rs.getSQLXML(i)),
      (s, ps, i, a) => ps.setSQLXML(i, bindingObject(s, a)),
      bindingObject
    )
  }

}