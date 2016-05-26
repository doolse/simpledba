package test

import io.doolse.simpledba.{MultiQuery, RelationMapper, SingleQuery, WriteQueries}
import shapeless._

/**
  * Created by jolz on 26/05/16.
  */

case class EmbeddedFields(adminpassword: String, enabled: Boolean)


case class Inst(uniqueid: Long, embedded: EmbeddedFields)

case class Username(fn: String, ln: String)
case class User(firstName: String, lastName: String, year: Int)

case class Queries[F[_]](instByPK: SingleQuery[F, Inst, Long],
                         writeInst: WriteQueries[F, Inst],
                         querybyFirstName: MultiQuery[F, User, String],
                         writeUsers: WriteQueries[F, User],
                         queryByLastName: MultiQuery[F, User, String],
                         queryByFullName: SingleQuery[F, User, Username]
                        )

object TestCreator {

  def createQueries[F[_], LC <: Poly0, C0 <: HList, CV0 <: HList, CZ <: HList](mapper: RelationMapper[F, LC]):
  (Queries[F], List[mapper.DDLStatement]) = {
//    implicit val embeddedColumnMapper = mapper.GenericColumnMapper[EmbeddedFields]
//    val instTable = mapper.relation[Inst]("institution").key('uniqueid)
//    val userTable = mapper.relation[User]("users").keys('firstName, 'lastName)
//
//    val allBuilders = HList(
//      mapper.queryByFullKey(instTable),
//      mapper.writeQueries(instTable),
//      mapper.queryAllByKeyColumn('firstName, userTable),
//      mapper.writeQueries(userTable),
//      mapper.queryAllByKeyColumn('lastName, userTable),
//      mapper.queryByFullKey(userTable)
//    )
//
//    val built = mapper.build(allBuilders)
//    val queries = built.as[Queries]()
//    val creation = built.ddl.value
//    (queries, creation)
    ???
  }
}
