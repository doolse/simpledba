package io.doolse.simpledba.dynamodb

import cats.data.State
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

sealed trait BinaryOperator
sealed trait UnaryOperator
sealed trait TrinaryOperator
sealed trait SortKeyOperator

case object EQ           extends BinaryOperator with SortKeyOperator
case object NE           extends BinaryOperator
case object LE           extends BinaryOperator with SortKeyOperator
case object LT           extends BinaryOperator with SortKeyOperator
case object GE           extends BinaryOperator with SortKeyOperator
case object GT           extends BinaryOperator with SortKeyOperator
case object BEGINS_WITH  extends BinaryOperator with SortKeyOperator
case object CONTAINS     extends BinaryOperator
case object NOT_CONTAINS extends BinaryOperator
case object NOT_NULL     extends UnaryOperator
case object NULL         extends UnaryOperator
case object BETWEEN      extends TrinaryOperator with SortKeyOperator

object DynamoDBExpression {
  case class ExprState(nameMap: Map[String, String], values: Map[String, AttributeValue])

  type Expr[A] = State[ExprState, A]

  def newAttr(preferred: String): Expr[String] = State.pure(preferred)
  def newValue(preferred: String, value: AttributeValue): Expr[String] = State[ExprState, String] {
    s =>
      val valId   = ":" + preferred
      val newVals = s.values.updated(valId, value)
      (s.copy(values = newVals), valId)
  }

  def binaryOpString(left: String, binaryOperator: BinaryOperator, right: String): String =
    binaryOperator match {
      case EQ          => s"$left = $right"
      case NE          => s"$left <> $right"
      case LE          => s"$left <= $right"
      case LT          => s"$left < $right"
      case GE          => s"$left >= $right"
      case GT          => s"$left > $right"
      case BEGINS_WITH => s"begins_with($left, $right)"
    }

  def keyExpression(pkName: String,
                        pkValue: AttributeValue,
                        sk: Option[(String, SortKeyOperator, Seq[AttributeValue])]): Expr[String] =
    for {
      pkVal        <- newValue("pk", pkValue)
      pkNameString <- newAttr(pkName)
    } yield binaryOpString(pkNameString, EQ, pkVal)
}

//EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN
