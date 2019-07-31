package com.kubukoz

import cats.effect._
import cats.implicits._
import cats.InvariantSemigroupal
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain

object datas {

  // def schema[A]: Schema[A] = ???
  def schema[A[_[_]]]: Schema[User] =
    //todo derivation
    Schema(TableName("users"), User[Column](Column.named("name"), Column.named("age")))

  final case class TableName(value: String) extends AnyVal

  final case class Schema[A[F[_]]](table: TableName, lifted: A[Column]) {
    def select[Queried](selection: A[Column] => Column[Queried]): Query[A, Queried] = Query(table, lifted, selection(lifted), Chain.empty)
  }

  import cats.Id

  def all[A[_[_]]]: Column[A[Id]] = Column.all
  def over[Tpe](l: Column[Tpe], r: Column[Tpe]): Filter = Filter(l.compileSQL ++ fr0" > " ++ r.compileSQL)
  def nonEqual[Tpe](l: Column[Tpe], r: Column[Tpe]): Filter = Filter(l.compileSQL ++ fr0" <> " ++ r.compileSQL)

  sealed trait Column[Tpe] extends Product with Serializable {

    def compileSQL: Fragment = this match {
      case Column.All()                                  => fr0"*"
      case Column.Named(name)                            => Fragment.const(name)
      case Column.Const(value, implicit0(put: Put[Tpe])) => fr0"$value"
      case p: Column.Product[_, _]                       => p.left.compileSQL ++ fr0", " ++ p.right.compileSQL
      case Column.IMap(underlying, _)                    => underlying.compileSQL
    }
  }

  object Column {
    final case class All[Tpe]() extends Column[Tpe]
    final case class Named[Tpe](name: String) extends Column[Tpe]
    final case class Const[Tpe](value: Tpe, into: Put[Tpe]) extends Column[Tpe]
    final case class Product[L, R](left: Column[L], right: Column[R]) extends Column[(L, R)]
    final case class IMap[A, B](underlying: Column[A], f: A => B) extends Column[B]

    def named[Tpe](name: String): Column[Tpe] = Named(name)
    def all[Tpe]: Column[Tpe] = All()
    def const[Tpe: Put](value: Tpe): Column[Tpe] = Const(value, Put[Tpe])

    implicit val invariant: InvariantSemigroupal[Column] = new InvariantSemigroupal[Column] {
      def imap[A, B](fa: Column[A])(f: A => B)(g: B => A): Column[B] = IMap(fa, f)
      def product[A, B](fa: Column[A], fb: Column[B]): Column[(A, B)] = Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](table: TableName, lifted: A[Column], selection: Column[Queried], filters: Chain[Filter]) {
    def where(filter: A[Column] => Filter): Query[A, Queried] = copy(filters = filters.append(filter(lifted)))

    def compileSql(implicit read: Read[Queried]): Query0[Queried] =
      (fr0"select " ++ selection.compileSQL ++ fr0" from " ++ Fragment.const(table.value) ++ Fragments.whereAnd(
        filters.toList.map(_.compileSql): _*
      )).query[Queried]
  }

  final case class Filter(compileSql: Fragment)
}

final case class User[F[_]](name: F[String], age: F[Int])

object Demo extends IOApp {
  import datas._

  val q =
    schema[User]
      .select(u => (all[User], u.name, u.age).tupled)
      .where(u => over(u.age, Column.const(18)))
      .where(u => nonEqual(u.name, Column.const("John")))

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def run(args: List[String]): IO[ExitCode] =
    IO {
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain.as(ExitCode.Success)
}
