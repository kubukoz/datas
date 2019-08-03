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
import cats.Applicative
import com.kubukoz.datas.Reference.Scoped
import cats.Monad
import cats.data.Reader
import cats.mtl.implicits._
import cats.Functor
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
import cats.data.Const

object datas {
  // def schema[A]: Schema[A] = ???

  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Column(col))
    }

    def caseClassSchema[F[_[_]]: FunctorK](table: TableName, stClass: ST[F[Reference]]): Schema[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (columns, data) => Schema(Table(table), data, Chain("u"), columns.toList)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(value: String) extends AnyVal

  final case class Table(name: TableName)

  final case class Schema[A[_[_]]: FunctorK](base: Table, lifted: A[Reference], scope: Scope, allColumns: ColumnList) {

    def select[Queried](selection: A[ScopedReference] => ScopedReference[Queried]): Query[A, Queried] =
      Query(
        base,
        lifted,
        selection(lifted.mapK(ScopedReference.liftReference(scope))).compile,
        scope,
        filters = Chain.empty,
        compiler = ReferenceCompiler.fromColumns(allColumns)
      )
  }

  import cats.Id

  //todo this should require a schema or querybase, but in a way that'll make it non-intrusive for users
  def all[A[_[_]]](a: A[ScopedReference]): Reference[A[Id]] = {
    val _ = a
    //todo wtf
    Reference.All(Scope.empty)
  }

  def over[Type](l: ScopedReference[Type], r: ScopedReference[Type]): Filter =
    binary(l, r)(_ ++ fr0" > " ++ _)

  def nonEqual[Type](l: ScopedReference[Type], r: ScopedReference[Type]): Filter = binary(l, r)(_ ++ fr0" <> " ++ _)

  def binary(l: ScopedReference[Any], r: ScopedReference[Any])(f: (Fragment, Fragment) => Fragment): Filter =
    Filter(compiler => f(compiler.compileReference(l.compile), compiler.compileReference(r.compile)))

  final case class Column(name: String) extends AnyVal

  final case class ScopedReference[+Type](scope: Scope, private val underlying: Reference[Type]) {
    def mapReference[BType](f: Reference[Type] => Reference[BType]): ScopedReference[BType] = ScopedReference(scope, f(underlying))
    def compile: Reference[Type] = Reference.Scoped(scope, underlying)
  }

  object ScopedReference {
    val compileK: ScopedReference ~> Reference = λ[ScopedReference ~> Reference](_.compile)
    def liftReference(scope: Scope): Reference ~> ScopedReference = λ[Reference ~> ScopedReference](ScopedReference(scope, _))

    implicit val invariant: InvariantSemigroupal[ScopedReference] = new InvariantSemigroupal[ScopedReference] {
      def imap[A, B](fa: ScopedReference[A])(f: A => B)(g: B => A): ScopedReference[B] = fa.mapReference(Reference.Widen[A, B](_))

      def product[A, B](fa: ScopedReference[A], fb: ScopedReference[B]): ScopedReference[(A, B)] =
        ScopedReference(Scope.empty, Reference.Product(fa.compile, fb.compile))
    }
  }

  sealed trait Reference[+Type] extends Product with Serializable

  object Reference {
    final case class All[Type](scope: Scope) extends Reference[Type]
    final case class Column[Type](col: datas.Column) extends Reference[Type]
    final case class Lift[Type](value: Type, into: Put[Type]) extends Reference[Type]
    final case class Raw[Type](sql: Fragment) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Widen[A, B](underlying: Reference[A]) extends Reference[B]
    final case class Scoped[Type](scope: Scope, underlying: Reference[Type]) extends Reference[Type]

    def column[Type](col: datas.Column): Reference[Type] = Column(col)
    def lift[Type: Put](value: Type): Reference[Type] = Lift(value, Put[Type])
    def raw[Type: Show](value: Fragment): Reference[Type] = Raw(value)

    def addScope(scope: Scope): Reference ~> Reference = λ[Reference ~> Reference](Scoped(scope, _))

    //todo unused for now
    implicit val invariant: InvariantSemigroupal[Reference] = new InvariantSemigroupal[Reference] {
      def imap[A, B](fa: Reference[A])(f: A => B)(g: B => A): Reference[B] = Reference.Widen(fa)

      def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] =
        Reference.Product(fa, fb)
    }
  }

  final case class Query[A[_[_]]: FunctorK, Queried](
    base: Table,
    lifted: A[Reference],
    selection: Reference[Queried],
    scope: Scope,
    filters: Chain[Filter],
    compiler: ReferenceCompiler
  ) {

    def where(filter: A[ScopedReference] => Filter): Query[A, Queried] =
      copy(filters = filters.append(filter(lifted.mapK(ScopedReference.liftReference(scope)))))

    def compileSql(implicit read: Read[Queried]): Query0[Queried] =
      (fr0"select " ++ compiler.compileReference(selection) ++ fr0" from " ++ Fragment.const(
        base.name.value + " " + scope.stringify
      ) ++ Fragments.whereAnd(
        filters.toList.map(_.compileSql(compiler)): _*
      )).query[Queried]
  }
  final case class Filter(compileSql: ReferenceCompiler => Fragment) {
    def and(another: Filter): Filter = Filter((compileSql, another.compileSql).mapN(Fragments.and(_, _)))
  }

  trait ReferenceCompiler {
    def compileReference(column: Reference[Any]): Fragment
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

  object ReferenceCompiler {

    def fromColumns(columns: ColumnList): ReferenceCompiler = new ReferenceCompiler {

      private def scopedFrag[F[_]: Scope.Ask: Functor](s: String): F[String] =
        Scope.Ask[F].ask.map(_.nonEmptyScope).map {
          case None      => s
          case Some(nes) => nes.stringify + "." + s
        }

      private def compileScoped[F[_]: Monad](reference: Reference[Any])(implicit L: Scope.Local[F]): F[Fragment] = reference match {
        case Reference.All(newScope) =>
          val compiledColumns =
            columns.toNel.map { _.map(Reference.column[Any]).reduceLeft(Reference.Product(_, _)) }.foldMapM(compileScoped[F])

          L.local(_.concat(newScope))(compiledColumns)

        case Reference.Column(column) => scopedFrag[F]("\"" + column.name + "\"").map(Fragment.const(_))
        case l: Reference.Lift[a] =>
          implicit val put: Put[a] = l.into
          val _ = put //to make scalac happy
          fr0"${l.value}".pure[F]
        case Reference.Raw(sql)           => sql.pure[F]
        case p: Reference.Product[_, _]   => (compileScoped[F](p.left), compileScoped[F](p.right)).mapN(_ ++ fr0", " ++ _)
        case Reference.Widen(underlying)  => compileScoped[F](underlying)
        case Scoped(newScope, underlying) => L.local(_.concat(newScope))(compileScoped[F](underlying))
      }

      def compileReference(reference: Reference[Any]): Fragment =
        compileScoped[Reader[Scope, ?]](reference).run(Chain.empty)
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
    caseClassSchema(TableName("books"), (column("id"), column("user_id")).mapN(Book[Reference])) //types explicit, less typing

  val userSchema: Schema[User] =
    caseClassSchema(
      TableName("users"),
      Applicative[schemas.ST].map3(column("id"), column("name"), column("age"))(User[Reference]) //types automatically inferred
    )

  val q =
    userSchema
      .select(u => ( /* all(u),  */ u.name, u.age).tupled)
      .where(u => over(u.age, ScopedReference(Scope.empty, Reference.lift(18))))
      .where(u => nonEqual(u.name, ScopedReference(Scope.empty, Reference.lift("John"))))

  val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver", "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")

  def run(args: List[String]): IO[ExitCode] =
    IO {
      println("\n\nstarting")
      println(q)
      println(q.compileSql.sql)
    } *> q.compileSql.stream.transact(xa).map(_.toString).showLinesStdOut.compile.drain *> IO(println("\n\n")).as(ExitCode.Success)
}
