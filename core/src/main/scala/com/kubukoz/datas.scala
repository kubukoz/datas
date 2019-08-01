package com.kubukoz

import cats.effect._
import cats.implicits._
import cats.InvariantSemigroupal
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.State
import cats.Show

object datas {

  // def schema[A]: Schema[A] = ???

  //todo derivation
  //todo make sure schema must have a table
  def schema[A[_[_]]]: Schema[User] = {
    type ST[X] = State[Chain[Column], X]

    def column[Tpe](name: String): ST[Reference[Tpe]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Column(col))
    }

    val schemaState: ST[User[Reference]] = (column[Long]("id"), column[String]("name"), column[Int]("age")).mapN(User[Reference])

    val (columns, data) = schemaState.run(Chain.empty).value

    Schema(QueryBase.Table(TableName("users")), data, columns.toList)
  }

  final case class TableName(value: String) extends AnyVal

  sealed trait QueryBase extends Product with Serializable

  object QueryBase {
    final case class Table(name: TableName) extends QueryBase
  }

  final case class Schema[A[F[_]]](base: QueryBase, lifted: A[Reference], allColumns: List[Column]) {

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(base, lifted, selection(lifted), Chain.empty, allColumns)
  }

  import cats.Id

  //todo this should require a schema or querybase, but in a way that'll make it non-intrusive for users
  //todo consider including columns in data structure (User)?
  def all[A[_[_]]]: Reference[A[Id]] = Reference.All()

  def over[Tpe](l: Reference[Tpe], r: Reference[Tpe]): Filter =
    binary(l, r)(_ ++ fr0" > " ++ _)

  def nonEqual[Tpe](l: Reference[Tpe], r: Reference[Tpe]): Filter = binary(l, r)(_ ++ fr0" <> " ++ _)

  def binary(l: Reference[Any], r: Reference[Any])(f: (Fragment, Fragment) => Fragment): Filter =
    Filter(compiler => f(compiler.compileReference(l), compiler.compileReference(r)))

  final case class Column(name: String) extends AnyVal

  sealed trait Reference[+Tpe] extends Product with Serializable

  object Reference {
    final case class All[Tpe]() extends Reference[Tpe]
    final case class Column[Tpe](col: datas.Column) extends Reference[Tpe]
    final case class Lift[Tpe](value: Tpe, into: Put[Tpe]) extends Reference[Tpe]
    final case class Raw[Tpe](sql: Fragment) extends Reference[Tpe]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Widen[A, B](underlying: Reference[A]) extends Reference[B]

    def column[Tpe](col: datas.Column): Reference[Tpe] = Column(col)
    def lift[Tpe: Put](value: Tpe): Reference[Tpe] = Lift(value, Put[Tpe])
    def raw[Tpe: Show](value: Fragment): Reference[Tpe] = Raw(value)

    implicit val invariant: InvariantSemigroupal[Reference] = new InvariantSemigroupal[Reference] {
      def imap[A, B](fa: Reference[A])(f: A => B)(g: B => A): Reference[B] = Widen(fa)
      def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: QueryBase,
    lifted: A[Reference],
    selection: Reference[Queried],
    filters: Chain[Filter],
    //todo this should somehow be bundled into base or the whole schema should be carried around
    allColumns: List[Column]
  ) {
    def where(filter: A[Reference] => Filter): Query[A, Queried] = copy(filters = filters.append(filter(lifted)))

    private val compiler: ReferenceCompiler = new ReferenceCompiler {

      def compileReference(reference: Reference[Any]): Fragment = reference match {
        case Reference.All() =>
          allColumns.toNel.fold(fr0"") { cols =>
            val allColumnProduct: Reference[Any] = cols.map(Reference.column[Any]).reduceLeft(Reference.Product(_, _))
            compileReference(allColumnProduct)
          }
        case Reference.Column(column) => Fragment.const(column.name)
        case l: Reference.Lift[a] =>
          implicit val put: Put[a] = l.into
          val _ = put //to make scalac happy
          fr0"${l.value}"
        case Reference.Raw(sql)          => sql
        case p: Reference.Product[_, _]  => compileReference(p.left) ++ fr0", " ++ compileReference(p.right)
        case Reference.Widen(underlying) => compileReference(underlying)
      }
    }

    def compileSql(implicit read: Read[Queried]): Query0[Queried] =
      (fr0"select " ++ compiler.compileReference(selection) ++ fr0" from " ++ Fragment.const(base.asInstanceOf[QueryBase.Table].name.value) ++ Fragments
        .whereAnd(
          filters.toList.map(_.compileSql(compiler)): _*
        )).query[Queried]
  }

  final case class Filter(compileSql: ReferenceCompiler => Fragment)

  trait ReferenceCompiler {
    def compileReference(column: Reference[Any]): Fragment
  }
}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object Demo extends IOApp {
  import datas._

  val q =
    schema[User]
      .select(u => (all[User], u.name, u.age).tupled)
      .where(u => over(u.age, Reference.lift(18)))
      .where(u => nonEqual(u.name, Reference.lift("John")))

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def run(args: List[String]): IO[ExitCode] =
    IO {
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain.as(ExitCode.Success)
}
