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
import cats.Applicative
import com.kubukoz.datas.Reference.Scoped
import cats.Monad
import cats.data.Reader
import cats.mtl.implicits._

object datas {

  // def schema[A]: Schema[A] = ???

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Column(col))
    }

    def caseClassSchema[F[_[_]]](table: TableName, stClass: ST[F[Reference]]): Schema[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (columns, data) => Schema(QueryBase.Table(table), data, "u", columns.toList)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(value: String) extends AnyVal

  sealed trait QueryBase extends Product with Serializable

  object QueryBase {
    final case class Table(name: TableName) extends QueryBase
  }

  final case class ScopedLifted[A[_[_]]](lifted: A[Reference], scope: String) {
    def field[Queried](path: A[Reference] => Reference[Queried]): Reference[Queried] = Reference.Scoped(scope, path(lifted))
  }

  final case class Schema[A[_[_]]](base: QueryBase, lifted: A[Reference], scope: String, allColumns: List[Column]) {

    def select[Queried](selection: ScopedLifted[A] => Reference[Queried]): Query[A, Queried] =
      Query(base, lifted, selection(ScopedLifted(lifted, scope)), scope, Chain.empty, allColumns)
  }

  import cats.Id

  //todo this should require a schema or querybase, but in a way that'll make it non-intrusive for users
  def all[A[_[_]]](a: ScopedLifted[A]): Reference[A[Id]] = {
    val _ = a
    Reference.All(a.scope)
  }

  def over[Type](l: Reference[Type], r: Reference[Type]): Filter =
    binary(l, r)(_ ++ fr0" > " ++ _)

  def nonEqual[Type](l: Reference[Type], r: Reference[Type]): Filter = binary(l, r)(_ ++ fr0" <> " ++ _)

  def binary(l: Reference[Any], r: Reference[Any])(f: (Fragment, Fragment) => Fragment): Filter =
    Filter(compiler => f(compiler.compileReference(l), compiler.compileReference(r)))

  final case class Column(name: String) extends AnyVal

  sealed trait Reference[+Type] extends Product with Serializable

  object Reference {
    final case class All[Type](scope: String) extends Reference[Type]
    final case class Column[Type](col: datas.Column) extends Reference[Type]
    final case class Lift[Type](value: Type, into: Put[Type]) extends Reference[Type]
    final case class Raw[Type](sql: Fragment) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Widen[A, B](underlying: Reference[A]) extends Reference[B]
    final case class Scoped[Type](scope: String, underlying: Reference[Type]) extends Reference[Type]

    def column[Type](col: datas.Column): Reference[Type] = Column(col)
    def lift[Type: Put](value: Type): Reference[Type] = Lift(value, Put[Type])
    def raw[Type: Show](value: Fragment): Reference[Type] = Raw(value)

    implicit val invariant: InvariantSemigroupal[Reference] = new InvariantSemigroupal[Reference] {
      def imap[A, B](fa: Reference[A])(f: A => B)(g: B => A): Reference[B] = Widen(fa)
      def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: QueryBase,
    lifted: A[Reference],
    selection: Reference[Queried],
    scope: String,
    filters: Chain[Filter],
    //todo this should somehow be bundled into base or the whole schema should be carried around
    allColumns: List[Column]
  ) {
    def where(filter: ScopedLifted[A] => Filter): Query[A, Queried] = copy(filters = filters.append(filter(ScopedLifted(lifted, scope))))

    def compileSql(implicit read: Read[Queried]): Query0[Queried] = {
      val compiler: ReferenceCompiler = ReferenceCompiler.fromColumns(allColumns)
      (fr0"select " ++ compiler.compileReference(selection) ++ fr0" from " ++ Fragment.const(
        base.asInstanceOf[QueryBase.Table].name.value + " " + scope
      ) ++ Fragments.whereAnd(
        filters.toList.map(_.compileSql(compiler)): _*
      )).query[Queried]
    }
  }
  final case class Filter(compileSql: ReferenceCompiler => Fragment) {
    def and(another: Filter): Filter = Filter((compileSql, another.compileSql).mapN(Fragments.and(_, _)))
  }

  trait ReferenceCompiler {
    def compileReference(column: Reference[Any]): Fragment
  }

  type Scope = Chain[String]

  object Scope {
    type Local[F[_]] = cats.mtl.ApplicativeLocal[F, Scope]
    def local[F[_]](implicit L: Local[F]): Local[F] = L
  }

  object ReferenceCompiler {

    def fromColumns(columns: List[Column]): ReferenceCompiler = new ReferenceCompiler {

      private def scopedFrag(scope: Chain[String])(s: String): String = scope.toList.toNel match {
        case None      => s
        case Some(nel) => nel.reduceLeft((a, b) => s"$a.$b") + "." + s
      }

      private def compileScoped[F[_]: Monad](reference: Reference[Any])(implicit L: Scope.Local[F]): F[Fragment] = reference match {
        case Reference.All(newScope) =>
          L.local(_.append(newScope)) {
            columns.toNel.map { _.map(Reference.column[Any]).reduceLeft(Reference.Product(_, _)) }.foldMapM(compileScoped[F])
          }

        case Reference.Column(column) =>
          L.reader { scope =>
            Fragment.const(scopedFrag(scope)("\"" + column.name + "\""))
          }
        case l: Reference.Lift[a] =>
          implicit val put: Put[a] = l.into
          val _ = put //to make scalac happy
          fr0"${l.value}".pure[F]
        case Reference.Raw(sql)           => sql.pure[F]
        case p: Reference.Product[_, _]   => (compileScoped[F](p.left), compileScoped[F](p.right)).mapN(_ ++ fr0", " ++ _)
        case Reference.Widen(underlying)  => compileScoped[F](underlying)
        case Scoped(newScope, underlying) => L.local(_.append(newScope))(compileScoped[F](underlying))
      }

      def compileReference(reference: Reference[Any]): Fragment =
        compileScoped[Reader[Scope, ?]](reference).run(Chain.empty)
    }
  }
}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])
final case class Book[F[_]](id: F[Long], userId: F[Long])

object Demo extends IOApp {
  import datas._

  import schemas.caseClassSchema
  import schemas.column

  val bookSchema: Schema[Book] =
    caseClassSchema(TableName("books"), (column[Long]("id"), column[Long]("user_id")).mapN(Book[Reference])) //types explicit, less typing

  val userSchema: Schema[User] =
    caseClassSchema(
      TableName("users"),
      Applicative[schemas.ST].map3(column("id"), column("name"), column("age"))(User[Reference]) //types automatically inferred
    )

  val q =
    userSchema
      .select(u => (all(u), u.field(u => (u.name, u.age).tupled)).tupled)
      .where(u => over(u.field(_.age), Reference.lift(18)))
      .where(u => nonEqual(u.field(_.name), Reference.lift("John")))

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def run(args: List[String]): IO[ExitCode] =
    IO {
      println("\n\nstarting")
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain *> IO(println("\n\n")).as(ExitCode.Success)
}
