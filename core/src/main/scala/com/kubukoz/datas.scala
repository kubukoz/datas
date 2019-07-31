package com.kubukoz

import cats.effect._
import cats.implicits._
import cats.InvariantSemigroupal

object datas {

  // def schema[A]: Schema[A] = ???
  def schema[A[_[_]]]: Schema[User] =
    //todo derivation
    Schema(TableName("user"), User[Column](Column.raw("name"), Column.raw("age")))

  final case class TableName(value: String) extends AnyVal

  final case class Schema[A[F[_]]](table: TableName, lifted: A[Column]) {
    def select[Queried](selection: A[Column] => Column[Queried]): Query[Queried] = Query(table, selection(lifted))
  }

  sealed trait Column[Tpe] extends Product with Serializable {

    def compileSQL: String = this match {
      case Column.Raw(name)           => name
      case Column.Const(value)        => value.toString //todo quote based on TC
      case p: Column.Product[_, _]    => (p.left.compileSQL + ", " + p.right.compileSQL)
      case Column.IMap(underlying, _) => underlying.compileSQL
    }
  }

  object Column {
    final case class Raw[Tpe](name: String) extends Column[Tpe]
    final case class Const[Tpe: Into](value: Tpe) extends Column[Tpe]
    final case class Product[L, R](left: Column[L], right: Column[R]) extends Column[(L, R)]
    final case class IMap[A, B](underlying: Column[A], f: A => B) extends Column[B]

    def raw[Tpe](name: String): Column[Tpe] = Raw(name)
    def const[Tpe: Into](value: Tpe): Column[Tpe] = Const(value)

    implicit val invariant: InvariantSemigroupal[Column] = new InvariantSemigroupal[Column] {
      def imap[A, B](fa: Column[A])(f: A => B)(g: B => A): Column[B] = IMap(fa, f)
      def product[A, B](fa: Column[A], fb: Column[B]): Column[(A, B)] = Product(fa, fb)
    }

    //fake typeclass for inserting things into queries
    trait Into[A] {
      def insert: A => String
    }
  }

  final case class Query[Queried](table: TableName, selection: Column[Queried]) {
    def compileSql: String = s"select ${selection.compileSQL} from ${table.value}"
  }
}

final case class User[F[_]](name: F[String], age: F[Int])

object Demo extends IOApp {
  import datas._
  val q = schema[User].select(u => (u.name, u.age).tupled)

  def run(args: List[String]): IO[ExitCode] =
    IO(
      println(q.compileSql)
    ).as(ExitCode.Success)
}
