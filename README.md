# simpledba - Simple Database Access for Scala

[![Build Status](https://api.travis-ci.org/doolse/simpledba.svg)](https://travis-ci.org/doolse/simpledba)

AKA principled column family access for Scala (highly inspired by [Doobie](https://github.com/tpolecat/doobie))
  
A database access library designed for easy access to Column Family based 
databases such as:

* Apache Cassandra
* Amazon DynamoDB
* Apache HBase

All of which are heavily inspired by Google's BigTable. 

## Quickstart

```scala
val simpledbaVersion = "0.1.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "io.doolse" %% "simpledba-cassandra",
  "io.doolse" %% "simpledba-circe"
).map(_ % simpledbaVersion)
```

```scala
import java.util.UUID

import io.doolse.simpledba._
import io.doolse.simpledba.cassandra._
import fs2.interop.cats._

object QuickstartExample extends App {
  case class User(userId: UUID, firstName: String, lastName: String, yearOfBirth: Int)
  
  case class Car(id: UUID, make: String, model: String, ownerId: UUID)

  case class Queries[F[_]](users: WriteQueries[F, User],
                           cars: WriteQueries[F, Car],
                           userByKey: UniqueQuery[F, User, UUID],
                           usersByFirstName: SortableQuery[F, User, String],
                           carsForUser: SortableQuery[F, Car, UUID]
                          )

  val model = RelationModel(
    relation[User]('user).key('userId),
    relation[Car]('car).key('id)
  ).queries[Queries](
    writes('user),
    writes('car),
    queryByPK('user),
    query('user).multipleByColumns('firstName),
    query('car).multipleByColumns('ownerId).sortBy('make)
  )

  val mapper = new CassandraMapper()
  val built = mapper.buildModel(model)
  val queries = built.queries
  ...
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

| cats| shapeless    | circe  | fs2    |cassandra-driver|dynamodb-driver|
| --- | ---          | ---    | ---    | ---     | ---    |
|0.6.0|2.3.2-SNAPSHOT|0.5.0-M1|0.9.0-M2|3.0.0    | 1.10.75|

