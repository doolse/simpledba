package test

import java.util.{Date, UUID}

import io.doolse.simpledba._
import shapeless._

/**
  * Created by jolz on 2/06/16.
  */
object RealisticModel {

  case class Queries()

  case class InstitutionId(id: Long)

  case class BaseEntityFields(uuid: UUID, institution: InstitutionId, owner: String, name: Map[String, String],
                              description: Map[String, String], created: Date, modified: Date)

  case class ItemCollection(bef: BaseEntityFields, schema: UUID, workflow: Option[UUID] = None, reviewperiod: Option[Int] = None)

  val model = RelationModel(
    HList(
      atom[Map[String, String], String]((_:Map[String, String]) => ???, (_:String) => ???),
      atom(Generic[InstitutionId]),
      embed[BaseEntityFields]
    ), HList(relation[ItemCollection]('id)), HList())
}
