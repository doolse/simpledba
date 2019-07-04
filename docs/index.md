# simpledba - Simple Database Access for Scala

[![Build Status](https://api.travis-ci.org/doolse/simpledba.svg)](https://travis-ci.org/doolse/simpledba)
[![Coverage Status](https://coveralls.io/repos/github/doolse/simpledba/badge.svg?branch=master)](https://coveralls.io/github/doolse/simpledba?branch=master)

AKA principled column family access for Scala (highly inspired by [Doobie](https://github.com/tpolecat/doobie))
  
A database access library designed for easy access to Column Family based 
databases such as:

* Apache Cassandra
* Amazon DynamoDB
* Apache HBase

All of which are heavily inspired by Google's BigTable. 

## Quickstart

@@@vars
```scala
val simpledbaVersion = "$project.version$"

libraryDependencies ++= Seq(
  "io.doolse" %% "simpledba-cassandra",
  "io.doolse" %% "simpledba-circe"
).map(_ % simpledbaVersion)
```
@@@

```scala
import java.util.UUID

import io.doolse.simpledba._
import io.doolse.simpledba.cassandra._

object QuickstartExample extends App {
  case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)

  case class Car(id: UUID, make: String, model: String, ownerId: UUID)

  case class Queries[F[_]](users: WriteQueries[F, User],
                           cars: WriteQueries[F, Car],
                           userByKey: UniqueQuery[F, User, UUID],
                           usersByFirstName: SortableQuery[F, User, String],
                           carsForUser: RangeQuery[F, Car, UUID, String]
                          )

  val userRel = relation[User]('user).key('userId)
  val carRel = relation[Car]('car).key('id)
  val model = RelationModel(userRel, carRel
  ).queries[Queries](
    writes(userRel),
    writes(carRel),
    queryByPK(userRel),
    query(userRel).multipleByColumns('firstName),
    query(carRel).multipleByColumns('ownerId).sortBy('make)
  )

  val mapper = new CassandraMapper()
  val built = mapper.buildModel(model)
  val queries = built.queries
  val sessionConfig = CassandraSession(CassandraIO.simpleSession("localhost"), c => Console.println(c()))
  CassandraUtils.initKeyspaceAndSchema(sessionConfig, "test", built.ddl, dropKeyspace = true).unsafeRunSync()

  private val magId = UUID.randomUUID()
  private val mahId = UUID.randomUUID()
  println {
    (for {
      _ <- queries.users.insert(User(magId, "Jolse", "Maginnis", 1980))
      _ <- queries.users.insert(User(mahId, "Jolse", "Mahinnis", 1999))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Accord Euro", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Honda", "Civic", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Ford", "Laser", magId))
      _ <- queries.cars.insert(Car(UUID.randomUUID(), "Hyundai", "Accent", magId))
      cars <- queries.carsForUser(magId, lower = Exclusive("Honda")).runLog
      users <- queries.usersByFirstName("Jolse").runLog
    } yield (cars ++ users).mkString("\n")).run(sessionConfig).unsafeRunSync()
  }
}
```
Which will produce CQL like:
```
	CREATE TABLE car(
		ownerId uuid, make text,
		id uuid, model text,
		PRIMARY KEY(ownerId, make, id))

	CREATE TABLE user(
		firstName text, userId uuid,
		lastName text, yearOfBirth int,
		PRIMARY KEY(firstName, userId))

	CREATE TABLE user_2(
		userId uuid, firstName text,
		lastName text, yearOfBirth int,
		PRIMARY KEY(userId))
		
INSERT INTO user_2 (userId,firstName,lastName,yearOfBirth) VALUES (6b59b1cd-a266-4b1d-a9ba-8228c5dec668,'Jolse','Maginnis',1980);
INSERT INTO user (userId,firstName,lastName,yearOfBirth) VALUES (6b59b1cd-a266-4b1d-a9ba-8228c5dec668,'Jolse','Maginnis',1980);
INSERT INTO car (id,make,model,ownerId) VALUES (74cf528b-2e1c-4e7b-8fba-cb2945170fb5,'Honda','Accord Euro',6b59b1cd-a266-4b1d-a9ba-8228c5dec668);
SELECT * FROM car WHERE ownerId=6b59b1cd-a266-4b1d-a9ba-8228c5dec668;
SELECT * FROM user WHERE firstName='Jolse';
```

## Simple == Scalable

If you're already sold on column family databases just skip ahead to the [Features](#Features).

For those only used to the flexibility of SQL databases, don't be fooled by any ridiculous attempts to dress
these types of databases up as if they are SQL-like (looking at you CQL!), you're best 
to think of them being the equivalent of an SQL database in which you can:

* Find a row by primary key
* Find multiple rows by part of the primary key and filtered/ordered by the rest of it.

That's a bit of a simplification but still that's a ridiculously limited subset of functionality right? 
What happens if I want to query by another column in the table, do I have to duplicate the data? 
Yep. Ouch! Painful. In essence your data model is dictated by the queries you need to do.
 
Why would anyone want to submit themselves to such limitations? Scalability, that's it. 
It becomes easy to spread your data out over a large cluster if you restrict to simple access like this.
Don't take my word for it : 
[https://www.quora.com/Which-companies-use-Cassandra](https://www.quora.com/Which-companies-use-Cassandra)

## <a name="#Features"></a>Features

* Define your data model as case classes, define which queries you need, let simpledba do the rest
* Same model can be used for all supported database types (easy to seamlessly change between them)
* Easy to define your own custom column types
* Integration with circe for easy custom JSON columns
* Multiple results are returned as functional streams (fs2)
* Optimised collection update operations when supported by backend DB

### Dependencies

| cats   | shapeless | circe  | fs2     |cassandra-driver|dynamodb-driver|
| ---    | ---       | ---    | ---     | ---            | ---    |
|1.0.0-MF|2.3.2      |0.5.0-M1|0.10.0-M6|3.0.0           | 1.10.75|

