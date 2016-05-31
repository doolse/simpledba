/**
  * Created by jolz on 31/05/16.
  */

import scala.util.parsing.combinator._

case class ParsedType(fqn: String, withs: List[ParsedType], params: List[ParsedType])

class ParseType extends RegexParsers {

  override def skipWhitespace = false

  def typeName : Parser[String] = """[\S&&[^\,\[\]]]*""".r ^^ (_.toString())

  lazy val argList : Parser[List[ParsedType]] = "[" ~> rep1sep(wholeType, ",") <~ "]"

  lazy val withType : Parser[ParsedType] = " with " ~> wholeType

  lazy val wholeType : Parser[ParsedType] = typeName ~ (argList ?) ~ (withType *) ^^ {
    case a ~ b ~ c => ParsedType(a, c, b.getOrElse(List.empty))
  }
}

object TestParser extends ParseType with App {

  lazy val printType: ParsedType => String = p => p.fqn match {
    case "shapeless.::" => printType(p.params.head) + " :: " + printType(p.params.tail.head)
    case "shapeless.labelled.FieldType" => printType(p.params.head) + " ->> " + printType(p.params.tail.head)
    case "shapeless.tag.Tagged" => printType(p.params.head)
    case "Symbol" => printType(p.withs.head)
    case o if p.withs.exists(_.fqn == "shapeless.tag.Tagged") => "FUCK"
    case o => if (p.params.isEmpty) o else o + p.params.map(printType).mkString("[", ",", "]")
  }

  val res = parse(wholeType, "shapeless.poly.Case2[test.TestDynamo.mapper.convertQueries.type,shapeless.::[test.TestDynamo.mapper.RelationDef[test.Inst,shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,Long] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"uniqueid\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,Long]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"adminpassword\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,Boolean] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"enabled\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,Boolean]],shapeless.HNil]]],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"uniqueid\")],shapeless.HNil],shapeless.::[Long,shapeless.::[String,shapeless.::[Boolean,shapeless.HNil]]]] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"institution\")],test.TestDynamo.mapper.RelationDef[test.Inst,shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,Long] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"uniqueid\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,Long]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"adminpassword\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.Inst,Boolean] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"enabled\")],test.TestDynamo.mapper.ColumnMapping[test.Inst,Boolean]],shapeless.HNil]]],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"uniqueid\")],shapeless.HNil],shapeless.::[Long,shapeless.::[String,shapeless.::[Boolean,shapeless.HNil]]]]],shapeless.::[test.TestDynamo.mapper.RelationDef[test.User,shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"firstName\")],test.TestDynamo.mapper.ColumnMapping[test.User,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"lastName\")],test.TestDynamo.mapper.ColumnMapping[test.User,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,Int] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"year\")],test.TestDynamo.mapper.ColumnMapping[test.User,Int]],shapeless.HNil]]],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"firstName\")],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"lastName\")],shapeless.HNil]],shapeless.::[String,shapeless.::[String,shapeless.::[Int,shapeless.HNil]]]] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"users\")],test.TestDynamo.mapper.RelationDef[test.User,shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"firstName\")],test.TestDynamo.mapper.ColumnMapping[test.User,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,String] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"lastName\")],test.TestDynamo.mapper.ColumnMapping[test.User,String]],shapeless.::[test.TestDynamo.mapper.ColumnMapping[test.User,Int] with shapeless.labelled.KeyTag[Symbol with shapeless.tag.Tagged[String(\"year\")],test.TestDynamo.mapper.ColumnMapping[test.User,Int]],shapeless.HNil]]],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"firstName\")],shapeless.::[Symbol with shapeless.tag.Tagged[String(\"lastName\")],shapeless.HNil]],shapeless.::[String,shapeless.::[String,shapeless.::[Int,shapeless.HNil]]]]],shapeless.HNil]],io.doolse.simpledba.FullKey[Symbol with shapeless.tag.Tagged[String(\"users\")]]]") match {
    case Success(p, _) => printType(p)
  }
  println(res)

//  println(parse(withType, " with G"))
}
