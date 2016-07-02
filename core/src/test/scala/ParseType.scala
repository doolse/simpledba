/**
  * Created by jolz on 31/05/16.
  */

import scala.io.StdIn
import scala.util.parsing.combinator._

case class ParsedType(fqn: String, withs: List[ParsedType], params: List[ParsedType])

class ParseType extends RegexParsers {

  override def skipWhitespace = false

  def typeParamName : Parser[String] = """\w*""".r
  def typeParamList : Parser[Unit] = "[" ~> repsep(typeParamName, ",") <~ "]" ^^ {
    case a => ()
  }

  def typeName : Parser[String] = opt(typeParamList) ~> """[\S&&[^\,\[\]]]*""".r

  lazy val argList : Parser[List[ParsedType]] = "[" ~> rep1sep(wholeType, ",") <~ "]"

  lazy val withType : Parser[ParsedType] = " with " ~> wholeType

  lazy val wholeType : Parser[ParsedType] = typeName ~ (argList ?) ~ (withType *) ^^ {
    case a ~ b ~ c => ParsedType(a, c, b.getOrElse(List.empty))
  }
}

object TestParser extends ParseType with App {

  def indent(i: Int): String = Range(0, i).map(_=>' ').mkString

  lazy val printRight: (Int, ParsedType) => String = (i,p) => p.fqn match {
    case "shapeless.::" => ",\n" + printType(i, p.params.head) + printRight(i, p.params.tail.head)
    case "shapeless.HNil" => s"\n${indent(i-1)})"
    case o => s"\nERROR-$o"
  }
  lazy val printType: (Int,ParsedType) => String = (i,p) => p.fqn match {
    case "shapeless.::" => indent(i) + "HList(\n" + printType(i+1, p.params.head) + printRight(i+1, p.params.tail.head)
    case "shapeless.HNil" => indent(i) + "HList()"
    case "shapeless.labelled.FieldType" => indent(i) + (printType(0, p.params.head) + " ->> " + printType(i, p.params.tail.head).trim())
    case "shapeless.tag.Tagged" => printType(i, p.params.head)
    case "Symbol" => printType(i, p.withs.head)
    case o if p.withs.exists(_.fqn == "shapeless.tag.Tagged") => "FUCK"
    case o if p.withs.exists(_.fqn == "shapeless.labelled.KeyTag") => {
      p.withs.find(_.fqn == "shapeless.labelled.KeyTag").map { kt =>
        printType(i, new ParsedType("shapeless.labelled.FieldType", List.empty, kt.params))
      }.getOrElse(sys.error("exists lied"))
    }
    case o => indent(i) + (if (p.params.isEmpty) o else o + p.params.map(p => printType(i+1,p)).mkString("[\n", ",\n", s"\n${indent(i)}]"))
  }

  def doRepl()
  {
    while (true) {
      val line = StdIn.readLine()
      val res = parse(wholeType, line) match {
        case Success(p, _) => printType(0, p)
        case o => o
      }
      println(res)
    }
  }

  doRepl()

//  println {
//    parse(typeParamName, "A]cats.data.Kleisli[fs2.util.Task,io.doolse.simpledba.cassandra.SessionConfig,A]")
//    parse(typeParamList, "[A]cats.data.Kleisli[fs2.util.Task,io.doolse.simpledba.cassandra.SessionConfig,A]")
//  }

}
