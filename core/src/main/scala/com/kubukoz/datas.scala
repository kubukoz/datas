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
import cats.data.Tuple2K
import cats.Apply

object datas {
  // def schema[A]: Schema[A] = ???

  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type: Read](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col), Read[Type]))
    }

    def caseClassSchema[F[_[_]]: FunctorK](table: TableName, stClass: ST[F[Reference]]): Schema[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (_, data) => Schema(QueryBase.SingleTable(Table(table)), data)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  sealed trait QueryBase extends Product with Serializable {

    def compileAsFrom(referenceCompiler: ReferenceCompiler): Fragment = this match {
      case QueryBase.SingleTable(table) => table.name.identifierFragment
      case QueryBase.LeftJoin(left, right, onClause) =>
        left.compileAsFrom(referenceCompiler) ++
          // Fragment.const(leftScope.stringify) ++
          fr"left join" ++
          right.compileAsFrom(referenceCompiler) ++
          // Fragment.const(rightScope.stringify) ++
          fr"on" ++
          onClause.compileSql(referenceCompiler)
    }
  }

  object QueryBase {
    final case class SingleTable(table: Table) extends QueryBase
    final case class LeftJoin(left: QueryBase, right: QueryBase, onClause: Filter) extends QueryBase
  }

  final case class Table(name: TableName)

  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F])

  object Tuple2KK {
    implicit def functorK[A[_[_]]: FunctorK, B[_[_]]: FunctorK]: FunctorK[Tuple2KK[A, B, ?[_]]] = new FunctorK[Tuple2KK[A, B, ?[_]]] {
      def mapK[F[_], G[_]](af: Tuple2KK[A, B, F])(fk: F ~> G): Tuple2KK[A, B, G] = Tuple2KK(af.left.mapK(fk), af.right.mapK(fk))
    }
  }

  final case class Schema[A[_[_]]: FunctorK](base: QueryBase, lifted: A[Reference]) {

    def leftJoin[B[_[_]]: FunctorK](another: Schema[B])(onClause: (A[Reference], B[Reference]) => Filter): Schema[Tuple2KK[A, B, ?[_]]] =
      Schema(
        QueryBase.LeftJoin(
          base,
          another.base,
          onClause(lifted, another.lifted)
        ),
        Tuple2KK(lifted, another.lifted)
      )

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(
        base,
        lifted,
        selection(lifted),
        filters = Chain.empty,
        compiler = ReferenceCompiler.fromColumns
      )
  }

  def over[Type](l: Reference[Type], r: Reference[Type]): Filter =
    binary(l.widen, r.widen)(_ ++ fr0" > " ++ _)

  def equal[Type](l: Reference[Type], r: Reference[Type]): Filter = binary(l.widen, r.widen)(_ ++ fr0" = " ++ _)
  def nonEqual[Type](l: Reference[Type], r: Reference[Type]): Filter = binary(l.widen, r.widen)(_ ++ fr0" <> " ++ _)

  def binary(l: Reference[Any], r: Reference[Any])(f: (Fragment, Fragment) => Fragment): Filter =
    Filter(compiler => f(compiler.compileReference(l).frag, compiler.compileReference(r).frag))

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {
    def widen[B >: Type]: ReferenceData[B] = this.asInstanceOf[ReferenceData[B]] //todo I'm pretty
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

    /* def addScope(scope: Scope): Reference ~> Reference = λ[Reference ~> Reference] {
      case Single(data, read)   => Single(ReferenceData.addScope(scope)(data), read)
      case Map(underlying, f)   => Map(addScope(scope)(underlying), f)
      case Product(left, right) => Product(addScope(scope)(left), addScope(scope)(right))
    }
     */
    implicit val invariant: Apply[Reference] = new Apply[Reference] {
      def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)
      def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] = product(ff, fa).map { case (f, a) => f(a) }
      override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Reference.Product(fa, fb)
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

      val compiledSelection = compiler.compileReference(selection)

      val frag = selectFrag ++ compiledSelection.frag ++ fr"from" ++ base.compileAsFrom(compiler) ++ whereFrag

      implicit val read: Read[Queried] = compiledSelection.read
      frag.query[Queried]
    }
  }
  final case class Filter(compileSql: ReferenceCompiler => Fragment) {
    def and(another: Filter): Filter = Filter((compileSql, another.compileSql).mapN(Fragments.and(_, _)))
  }

  trait ReferenceCompiler {
    def compileReference[Type](column: Reference[Type]): TypedFragment[Type]
  }

  final case class TypedFragment[Type](frag: Fragment, read: Read[Type]) {
    def map[B](f: Type => B): TypedFragment[B] = copy(read = read.map(f))
  }

  object ReferenceCompiler {

    def fromColumns: ReferenceCompiler = new ReferenceCompiler {
      private def compileScoped[F[_]: Applicative, Type](reference: Reference[Type]): F[TypedFragment[Type]] =
        reference match {
          case imap: Reference.Map[a, b] =>
            compileScoped[F, a](imap.underlying).map(_.map(imap.f))
          case product: Reference.Product[a, b] =>
            (compileScoped[F, a](product.left), compileScoped[F, b](product.right)).mapN { (l, r) =>
              implicit val rl = l.read
              implicit val rr = r.read
              val _ = (rl, rr)
              TypedFragment[(a, b)](l.frag ++ fr0", " ++ r.frag, Read[(a, b)])
            }

          case Reference.Single(data, read) =>
            compileScopedData[F, Type](read).apply(data)
        }

      private def compileScopedData[F[_]: Applicative, Type](read: Read[Type]): ReferenceData[Type] => F[TypedFragment[Type]] = {
        case ReferenceData.Column(column) =>
          /* scopedFrag[F]("\"" + column.name + "\"").map(Fragment.const(_)).map(TypedFragment[Type](_, read)) */
          TypedFragment[Type](Fragment.const("\"" + column.name + "\""), read).pure[F]
        case l: ReferenceData.Lift[a] =>
          implicit val put = l.into
          val _ = put //to make scalac happy
          TypedFragment[Type](fr0"${l.value}", read).pure[F]
      }

      def compileReference[Type](reference: Reference[Type]): TypedFragment[Type] =
        compileScoped[cats.Id, Type](reference)
    }
  }
}

final case class User[F[_]](id: F[Long], name: F[String], age: F[Int])

object User {
  implicit val functorK: FunctorK[User] = new FunctorK[User] {
    def mapK[F[_], G[_]](af: User[F])(fk: F ~> G): User[G] = User(fk(af.id), fk(af.name), fk(af.age))
  }
}

final case class Book[F[_]](id: F[Long], userId: F[Long])

object Book {
  implicit val functorK: FunctorK[Book] = new FunctorK[Book] {
    def mapK[F[_], G[_]](af: Book[F])(fk: F ~> G): Book[G] = Book(fk(af.id), fk(af.userId))
  }
}

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

  val q1 =
    userSchema
      .select(u => (u.name, Reference.lift(true), u.age.map[Boolean](_ => false)).tupled)
      .where(u => over(u.age, Reference.lift(18)))
      .where(u => nonEqual(u.name, Reference.lift("John")))

  val q =
    userSchema
      .leftJoin(bookSchema) { (u, b) =>
        equal(u.id, b.userId)
      }
      .leftJoin(bookSchema) { (u, b) =>
        equal(u.right.id, b.userId)
      }
      .select {
        case t =>
          (t.left.left.age, t.left.left.name, t.left.right.userId, t.right.id).tupled
      }
      .where { t =>
        equal(t.left.left.age, Reference.lift(18)) and equal(t.right.id, Reference.lift(1L))
      }

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def getAll[A[_[_]], Queried](q: Query[A, Queried]): IO[Unit] =
    IO {
      println("\n\nstarting")
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain *> IO(println("\n\n"))

  def run(args: List[String]): IO[ExitCode] =
    getAll(q1) *> getAll(q).as(ExitCode.Success)
}
