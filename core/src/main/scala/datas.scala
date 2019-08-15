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
import shapeless.HNil
import shapeless.{:: => HCons}
import datas.QueryBase.FromTable
import datas.QueryBase.Join

object datas {
  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type: Read](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col, None), Read[Type]))
    }

    def caseClassSchema[F[_[_]]: FunctorK](name: TableName, stClass: ST[F[Reference]]): TableQuery[F] =
      stClass
        .run(Chain.empty)
        .map {
          case (_, data) =>
            TableQuery(QueryBase.FromTable(name), data)
        }
        .value
  }
  //todo derivation
  //todo make sure schema must have a table

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  sealed trait QueryBase extends Product with Serializable {

    private def doCompile(qbase: QueryBase, currentIndex: Int): (Fragment, Int) =
      qbase match {
        case FromTable(table) => (table.identifierFragment ++ Fragment.const("x" + currentIndex), currentIndex + 1)
        case Join(left, right, kind, onClause, ll, rr) =>
          val (leftFrag, indexAfterLeft) = doCompile(left, currentIndex)

          val (rightFrag, indexAfterRight) = doCompile(right, indexAfterLeft)

          val joinFrag = leftFrag ++
            Fragment.const(kind) ++
            rightFrag ++
            fr"on" ++
            onClause(ll(currentIndex), rr(indexAfterLeft)).compile.frag

          (joinFrag, indexAfterRight)
      }

    def compileAsFrom: Fragment =
      doCompile(this, 0)._1
  }

  object QueryBase {
    final case class FromTable(table: TableName) extends QueryBase
    final case class Join[A[_[_]], B[_[_]]](
      left: QueryBase,
      right: QueryBase,
      kind: JoinKind,
      onClause: (A[Reference], B[Reference]) => Reference[Boolean],
      leftLifted: Int => A[Reference],
      rightLifted: Int => B[Reference]
    ) extends QueryBase
  }

  type JoinKind = String

  type JoinedTableQuery[A[_[_]], B[_[_]]] = TableQuery[Tuple2KK[A, B, ?[_]]]

  final case class TableQuery[A[_[_]]: FunctorK](base: QueryBase, lifted: A[Reference]) {

    def innerJoin[B[_[_]]: FunctorK](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): JoinedTableQuery[A, B] = join(another, "inner join")(onClause)

    def join[B[_[_]]: FunctorK](
      another: TableQuery[B],
      kind: JoinKind
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): JoinedTableQuery[A, B] =
      TableQuery(
        QueryBase
          .Join(base, another.base, kind, onClause, i => lifted.mapK(setScope("x" + i)), i => another.lifted.mapK(setScope("x" + i))),
        Tuple2KK(
          lifted,
          another.lifted
        )
      )

    private def setScope(scope: String) = Reference.mapData {
      λ[ReferenceData ~> ReferenceData] {
        case ReferenceData.Column(n, None) =>
          ReferenceData.Column(n, Some(scope))
        case c @ ReferenceData.Column(_, Some(_)) =>
          println("ignoring already defined scope! " + c + ", " + scope)
          c
        case c => c
      }
    }

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(base, lifted, selection, filters = Chain.empty)
  }

  final case class Column(name: String) extends AnyVal

  sealed trait ReferenceData[Type] extends Product with Serializable {

    def widen[B >: Type]: ReferenceData[B] =
      this.asInstanceOf[ReferenceData[B]] //todo I'm pretty sure it won't work for some cases
  }

  object ReferenceData {
    final case class Column[A](col: datas.Column, scope: Option[String]) extends ReferenceData[A]
    final case class Lift[Type](value: Type, into: Param[Type HCons HNil]) extends ReferenceData[Type]
    final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]
  }

  sealed trait Reference[Type] extends Product with Serializable {

    def compile: TypedFragment[Type] =
      ReferenceCompiler.default.compileReference(this)
  }

  object Reference {
    final case class Single[Type](data: ReferenceData[Type], read: Read[Type]) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

    def lift[Type: Read](value: Type)(implicit param: Param[Type HCons HNil]): Reference[Type] =
      Reference.Single[Type](ReferenceData.Lift(value, param), Read[Type])

    def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
      λ[Reference ~> Reference] {
        case Single(data, read) => Single(fk(data), read)
        case Map(underlying, f) => Map(mapData(fk)(underlying), f)
        case Product(left, right) =>
          Product(mapData(fk)(left), mapData(fk)(right))
      }

    implicit val invariant: Apply[Reference] = new Apply[Reference] {
      def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)

      def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] =
        product(ff, fa).map { case (f, a) => f(a) }
      override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] =
        Reference.Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: QueryBase,
    lifted: A[Reference],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter))

    def compileSql: Query0[Int] = {
      val compiledSelection = fr"1" //selection(lifted).compile

      // implicit val read: Read[Queried] = compiledSelection.read

      val frag = fr"select" ++ compiledSelection /* .frag */ ++
        fr"from" ++ base.compileAsFrom ++
        Fragments.whereAnd(
          /* filters.map(_.apply(lifted).compile.frag).toList: _* */
          fr"true"
        )

      // frag.query[Queried]
      frag.query[Int]

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
          case Reference.Single(data, read) =>
            compileData[F, Type](read).apply(data)
          case m: Reference.Map[a, b] =>
            compileScoped[F, a](m.underlying).map(_.map(m.f))
          case p: Reference.Product[a, b] =>
            (compileScoped[F, a](p.left), compileScoped[F, b](p.right)).mapN(_ product _)
        }

      private def compileData[F[_]: Applicative, Type](read: Read[Type]): ReferenceData[Type] => F[TypedFragment[Type]] = {
        case ReferenceData.Column(column, scope) =>
          val scopeString = scope.foldMap(_ + ".")
          TypedFragment[Type](
            Fragment.const(scopeString + "\"" + column.name + "\""),
            read
          ).pure[F]
        case l: ReferenceData.Lift[a] =>
          implicit val param: Param[a HCons HNil] = l.into
          val _ = param //to make scalac happy
          TypedFragment[Type](fr"${l.value}", read).pure[F]
        case r: ReferenceData.Raw[a] =>
          TypedFragment[Type](r.fragment, read).pure[F]
      }

      def compileReference[Type](reference: Reference[Type]): TypedFragment[Type] =
        compileScoped[cats.Id, Type](reference)
    }
  }

  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }

  object Tuple2KK {
    implicit def functorK[A[_[_]]: FunctorK, B[_[_]]: FunctorK]: FunctorK[Tuple2KK[A, B, ?[_]]] = new FunctorK[Tuple2KK[A, B, ?[_]]] {
      def mapK[F[_], G[_]](af: Tuple2KK[A, B, F])(fk: F ~> G): Tuple2KK[A, B, G] = Tuple2KK(af.left.mapK(fk), af.right.mapK(fk))
    }
  }

  def over[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr">" ++ _)

  def equal[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def equalOptionL[Type]: (Reference[Option[Type]], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def notNull[Type]: Reference[Option[Type]] => Reference[Boolean] =
    a =>
      Reference.Single(
        ReferenceData.Raw(a.compile.frag ++ fr"is not null"),
        Read[Boolean]
      )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.frag, r.compile.frag)),
      Read[Boolean]
    )
}
