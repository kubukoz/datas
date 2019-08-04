package com.kubukoz

import cats.effect._
import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.State
import cats.Applicative
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
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

    def compileAsFrom: Fragment = this match {
      case QueryBase.SingleTable(table) => table.name.identifierFragment
      case QueryBase.LeftJoin(left, right, onClause) =>
        left.compileAsFrom ++
          // Fragment.const(leftScope.stringify) ++
          fr"left join" ++
          right.compileAsFrom ++
          // Fragment.const(rightScope.stringify) ++
          fr"on" ++
          onClause.compile.frag
    }
  }

  object QueryBase {
    final case class SingleTable(table: Table) extends QueryBase
    final case class LeftJoin(left: QueryBase, right: QueryBase, onClause: Reference[Boolean]) extends QueryBase
  }

  final case class Table(name: TableName)

  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  object Tuple2KK {
    implicit def functorK[A[_[_]]: FunctorK, B[_[_]]: FunctorK]: FunctorK[Tuple2KK[A, B, ?[_]]] = new FunctorK[Tuple2KK[A, B, ?[_]]] {
      def mapK[F[_], G[_]](af: Tuple2KK[A, B, F])(fk: F ~> G): Tuple2KK[A, B, G] = Tuple2KK(af.left.mapK(fk), af.right.mapK(fk))
    }
  }

  final case class Schema[A[_[_]]: FunctorK](base: QueryBase, lifted: A[Reference]) {

    def leftJoin[B[_[_]]: FunctorK](
      another: Schema[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): Schema[Tuple2KK[A, B, ?[_]]] =
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
        filters = Chain.empty
      )
  }

  def over[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] = binary(_ ++ fr">" ++ _)
  def equal[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] = binary(_ ++ fr"=" ++ _)
  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] = binary(_ ++ fr"<>" ++ _)

  def binary[Type](f: (Fragment, Fragment) => Fragment)(l: Reference[Type], r: Reference[Type]): Reference[Boolean] =
    Reference.Single(ReferenceData.Raw(f(l.compile.frag, r.compile.frag)), Read[Boolean])

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {
    def widen[B >: Type]: ReferenceData[B] = this.asInstanceOf[ReferenceData[B]] //todo I'm pretty
  }

  object ReferenceData {
    final case class Column[A](col: datas.Column) extends ReferenceData[A]
    final case class Lift[Type](value: Type, into: Put[Type]) extends ReferenceData[Type]
    final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]
  }

  sealed trait Reference[Type] extends Product with Serializable {
    def compile: TypedFragment[Type] = ReferenceCompiler.default.compileReference(this)
  }

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
    filters: Chain[Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter(lifted)))

    def compileSql: Query0[Queried] = {
      val compiledSelection = selection.compile

      implicit val read: Read[Queried] = compiledSelection.read

      val frag = fr"select" ++ compiledSelection.frag ++
        fr"from" ++ base.compileAsFrom ++
        Fragments.whereAnd(filters.map(_.compile.frag).toList: _*)

      frag.query[Queried]
    }
  }

  trait ReferenceCompiler {
    def compileReference[Type](column: Reference[Type]): TypedFragment[Type]
  }

  final case class TypedFragment[Type](frag: Fragment, read: Read[Type]) {
    def map[B](f: Type => B): TypedFragment[B] = copy(read = read.map(f))

    def product[B](another: TypedFragment[B]): TypedFragment[(Type, B)] = {
      implicit val rl = read
      implicit val rr = another.read
      val _ = (rl, rr)
      TypedFragment(frag ++ fr"," ++ another.frag, Read[(Type, B)])
    }
  }

  object ReferenceCompiler {

    val default: ReferenceCompiler = new ReferenceCompiler {
      private def compileScoped[F[_]: Applicative, Type](reference: Reference[Type]): F[TypedFragment[Type]] =
        reference match {
          case Reference.Single(data, read) => compileData[F, Type](read).apply(data)
          case m: Reference.Map[a, b]       => compileScoped[F, a](m.underlying).map(_.map(m.f))
          case p: Reference.Product[a, b]   => (compileScoped[F, a](p.left), compileScoped[F, b](p.right)).mapN(_ product _)
        }

      private def compileData[F[_]: Applicative, Type](read: Read[Type]): ReferenceData[Type] => F[TypedFragment[Type]] = {
        case ReferenceData.Column(column) =>
          /* scopedFrag[F]("\"" + column.name + "\"").map(Fragment.const(_)).map(TypedFragment[Type](_, read)) */
          TypedFragment[Type](Fragment.const("\"" + column.name + "\""), read).pure[F]
        case l: ReferenceData.Lift[a] =>
          implicit val put = l.into
          val _ = put //to make scalac happy
          TypedFragment[Type](fr"${l.value}", read).pure[F]
        case r: ReferenceData.Raw[a] =>
          TypedFragment[Type](r.fragment, read).pure[F]
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
      .select(
        u =>
          (
            u.name,
            Reference.lift(true),
            u.age.as(false),
            equal(u.age, Reference.lift(23)),
            equal(Reference.lift(5), Reference.lift(10))
          ).tupled
      )
      .where(u => over(u.age, Reference.lift(18)))
      .where(u => nonEqual(u.name, Reference.lift("John")))

  val q =
    userSchema
      .leftJoin(bookSchema)((u, b) => equal(u.id, b.userId))
      .leftJoin(bookSchema)((u, b) => equal(u.right.id, b.id))
      .select {
        _.asTuple.leftMap(_.asTuple) match {
          case ((user, book1), book2) => (user.age, user.name, book1.userId, book2.id, user.id).tupled
        }
      }
      .where {
        _.asTuple.leftMap(_.asTuple) match {
          case ((user, _), _) => equal(user.age, Reference.lift(18))
        }
      }
      .where(t => equal(t.right.id, Reference.lift(1L)))

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
