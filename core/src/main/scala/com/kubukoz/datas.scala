package com.kubukoz

import cats.effect._
import cats.implicits._
import cats.InvariantSemigroupal
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.NonEmptyChain
import cats.data.State
import cats.Show
import cats.Functor
import cats.Applicative
import cats.Monad
import cats.data.Reader
import cats.mtl.implicits._
import cats.Functor
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
import cats.data.Const
import com.kubukoz.datas.QueryBase.SingleTable
import com.kubukoz.datas.Reference.Single

object datas {
  // def schema[A]: Schema[A] = ???

  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type: Read](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col), Read[Type]))
    }

    def caseClassSchema[F[_[_]]](table: TableName, stClass: ST[F[Reference]]): Schema[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (columns, data) => Schema(QueryBase.SingleTable(Table(table)), data, Chain("u"), columns.toList)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  sealed trait QueryBase extends Product with Serializable {

    def compileAsFrom: Fragment = this match {
      case SingleTable(table) => fr"from" ++ table.name.identifierFragment
      // case LeftJoin(left, right) => ???
    }
  }

  object QueryBase {
    final case class SingleTable(table: Table) extends QueryBase
    // final case class LeftJoin(left: Table, right: Table) extends QueryBase
  }

  final case class Table(name: TableName)

  final case class Schema[A[_[_]]](base: QueryBase, lifted: A[Reference], scope: Scope, allColumns: ColumnList) {

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(
        base,
        lifted,
        selection(lifted),
        filters = Chain.empty,
        compiler = ReferenceCompiler.fromColumns(allColumns)
      )
  }

  // def over[Type](l: Reference[Type], r: Reference[Type]): Filter =
  //   binary(l.widen, r.widen)(_ ++ fr0" > " ++ _)

  // def nonEqual[Type](l: Reference[Type], r: Reference[Type]): Filter = binary(l.widen, r.widen)(_ ++ fr0" <> " ++ _)

  def binary(l: Reference[Any], r: Reference[Any])(f: (Fragment, Fragment) => Fragment): Filter =
    Filter(compiler => f(compiler.compileReference(l).frag, compiler.compileReference(r).frag))

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {
    def widen[B >: Type]: ReferenceData[B] = this.asInstanceOf[ReferenceData[B]] //todo I'm pretty sure
  }

  object ReferenceData {
    final case class Column[A](col: datas.Column) extends ReferenceData[A]
    final case class Lift[Type](value: Type, into: Put[Type]) extends ReferenceData[Type]
  }

  sealed trait Reference[Type] extends Product with Serializable

  object Reference {
    final case class Single[Type](data: ReferenceData[Type], read: Read[Type]) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

    def lift[Type: Put: Read](value: Type): Reference[Type] = Reference.Single[Type](ReferenceData.Lift(value, Put[Type]), Read[Type])

    implicit val invariant: Functor[Reference] with InvariantSemigroupal[Reference] = new Functor[Reference]
    with InvariantSemigroupal[Reference] {
      def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)
      def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Reference.Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: QueryBase,
    lifted: A[Reference],
    selection: Reference[Queried],
    filters: Chain[Filter],
    compiler: ReferenceCompiler
  ) {

    def where(filter: A[Reference] => Filter): Query[A, Queried] =
      copy(filters = filters.append(filter(lifted)))

    def compileSql: Query0[Queried] = {
      val selectFrag = fr0"select "
      val whereFrag = Fragments.whereAnd(filters.map(_.compileSql(compiler)).toList: _*)

      val compiled = compiler.compileReference(selection)

      val frag = selectFrag ++ compiled.frag ++ base.compileAsFrom ++ whereFrag

      implicit val read: Read[Queried] = compiled.read
      frag.query[Queried]
    }
  }
  final case class Filter(compileSql: ReferenceCompiler => Fragment) {
    def and(another: Filter): Filter = Filter((compileSql, another.compileSql).mapN(Fragments.and(_, _)))
  }

  trait ReferenceCompiler {
    def compileReference[Type](column: Reference[Type]): TypedFragment[Type]
  }

  type Scope = Chain[String]
  type NonEmptyScope = NonEmptyChain[String]

  implicit class ScopeStringify(private val scope: Scope) extends AnyVal {
    def stringify: String = nonEmptyScope.foldMap(_.stringify)
    def nonEmptyScope: Option[NonEmptyScope] = NonEmptyChain.fromChain(scope)
  }

  implicit class NonEmptyScopeStringify(private val nes: NonEmptyScope) extends AnyVal {
    def stringify: String = nes.mkString_(".")
  }

  object Scope {
    val empty: Scope = Chain.empty
    type Ask[F[_]] = cats.mtl.ApplicativeAsk[F, Scope]
    def Ask[F[_]](implicit F: Ask[F]): Ask[F] = F
    type Local[F[_]] = cats.mtl.ApplicativeLocal[F, Scope]
    def local[F[_]](implicit L: Local[F]): Local[F] = L
  }

  final case class TypedFragment[Type](frag: Fragment, read: Read[Type]) {
    def map[B](f: Type => B): TypedFragment[B] = copy(read = read.map(f))
  }

  object ReferenceCompiler {

    def fromColumns(columns: ColumnList): ReferenceCompiler = new ReferenceCompiler {

      private def scopedFrag[F[_]: Scope.Ask: Functor](s: String): F[String] =
        Scope.Ask[F].ask.map(_.nonEmptyScope).map {
          case None      => s
          case Some(nes) => nes.stringify + "." + s
        }

      private def compileScoped[F[_]: Monad, Type](reference: Reference[Type])(implicit L: Scope.Local[F]): F[TypedFragment[Type]] =
        reference match {
          case imap: Reference.Map[a, b] =>
            compileScoped[F, a](imap.underlying).map(_.map(imap.f)) //todo g unused? makes sense
          case product: Reference.Product[a, b] =>
            val left = compileScoped[F, a](product.left)
            val right = compileScoped[F, b](product.right)

            (left, right).mapN { (l, r) =>
              implicit val rl = l.read
              implicit val rr = r.read
              val _ = (rl, rr)
              TypedFragment[(a, b)](l.frag ++ fr0", " ++ r.frag, Read[(a, b)])
            }

          case Reference.Single(data, read) =>
            data match {
              case ReferenceData.Column(column) =>
                scopedFrag[F]("\"" + column.name + "\"").map(Fragment.const(_)).map(TypedFragment[Type](_, read))
              case l: ReferenceData.Lift[a] =>
                implicit val put = l.into
                val _ = put //to make scalac happy
                TypedFragment[Type](fr0"${l.value}", read).pure[F]
            }
        }

      def compileReference[Type](reference: Reference[Type]): TypedFragment[Type] =
        compileScoped[Reader[Scope, ?], Type](reference).run(Chain.empty)
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
      Applicative[schemas.ST]
        .map3(column[Long]("id"), column[String]("name"), column[Int]("age"))(User[Reference]) //types automatically inferred
    )

  val q =
    userSchema.select(u => (u.name, Reference.lift(true), u.age.map[Boolean](_ => false)).tupled)
  // .select(u => u.age.imap[Boolean](_ => false)(_ => 1))
  // .where(u => over(u.age, Reference.lift(18)))
  // .where(u => nonEqual(u.name, Reference.lift("John")))

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def run(args: List[String]): IO[ExitCode] =
    IO {
      println("\n\nstarting")
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain *> IO(println("\n\n")).as(ExitCode.Success)
}
