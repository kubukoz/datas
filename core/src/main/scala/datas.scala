import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.data.State
import cats.tagless.FunctorK
import cats.~>
import cats.tagless.implicits._
import cats.Apply
import shapeless.HNil
import shapeless.{:: => HCons}
import cats.mtl.MonadState
import cats.mtl.instances.all._
import cats.FlatMap
import cats.data.OptionT

object datas {
  type ColumnList = List[Column]

  object schemas {
    type ST[X] = State[Chain[Column], X]

    def column[Type: Read: Get](name: String): ST[Reference[Type]] = {
      val col = Column(name)

      State.modify[Chain[Column]](_.append(col)).as(Reference.Single(ReferenceData.Column(col, None), Get[Type]))
    }

    def caseClassSchema[F[_[_]]: FunctorK](name: TableName, stClass: ST[F[Reference]]): TableQuery[F] =
      stClass.runA(Chain.empty).map(TableQuery.FromTable(name, _, FunctorK[F])).value
  }

  final case class TableName(name: String) extends AnyVal {
    def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
  }

  type IndexState[F[_]] = MonadState[F, Int]

  object IndexState {
    def apply[F[_]](implicit F: IndexState[F]): IndexState[F] = F
    def getAndInc[F[_]: IndexState: Apply]: F[Int] = IndexState[F].get <* IndexState[F].modify(_ + 1)
  }

  sealed trait TableQuery[A[_[_]]] extends Product with Serializable {

    def compileAsFrom: (Fragment, A[Reference]) =
      TableQuery.doCompile[A, State[Int, *]](this).runA(0).value

    def innerJoin[B[_[_]]](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[JoinKind.Inner[A, B]#Joined] =
      join(another, JoinKind.inner[A, B])(onClause)

    def leftJoin[B[_[_]]: FunctorK](
      another: TableQuery[B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[JoinKind.Left[A, B]#Joined] =
      join(another, JoinKind.left[A, B])(onClause)

    def join[B[_[_]]](
      another: TableQuery[B],
      kind: JoinKind[A, B]
    )(
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ): TableQuery[kind.Joined] =
      TableQuery.Join(this, another, kind, onClause)

    def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
      Query(this, selection, filters = Chain.empty)
  }

  object TableQuery {
    final case class FromTable[A[_[_]]](table: TableName, lifted: A[Reference], functorK: FunctorK[A]) extends TableQuery[A]
    final case class Join[A[_[_]], B[_[_]], K <: JoinKind[A, B]](
      left: TableQuery[A],
      right: TableQuery[B],
      kind: K,
      onClause: (A[Reference], B[Reference]) => Reference[Boolean]
    ) extends TableQuery[K#Joined]

    /**
      * Returns: the compiled query base (from + joins) and the scoped references underlying it (passed later to selections and filters).
      */
    def doCompile[A[_[_]], F[_]: IndexState: FlatMap](qbase: TableQuery[A]): F[(Fragment, A[Reference])] =
      qbase match {
        case t: FromTable[t] =>
          implicit val functorK = t.functorK
          IndexState.getAndInc[F].map { index =>
            val scope = t.table.name + "_x" + index
            (
              t.table.identifierFragment ++ Fragment.const(scope),
              t.lifted.mapK(setScope(scope))
            )
          }

        case j: Join[a, b, k] =>
          import j._
          (doCompile[a, F](left), doCompile[b, F](right)).mapN {
            case ((leftFrag, leftCompiledReference), (rightFrag, rightCompiledReference)) =>
              val thisJoinClause = onClause(leftCompiledReference, rightCompiledReference).compile.compile._1

              val joinFrag = leftFrag ++ Fragment.const(kind.kind) ++ rightFrag ++ fr"on" ++ thisJoinClause

              (joinFrag, kind.buildJoined(leftCompiledReference, rightCompiledReference))
          }
      }

  }

  trait JoinKind[A[_[_]], B[_[_]]] {
    type Joined[F[_]]
    def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference]
    def kind: String
  }

  object JoinKind {
    type Aux[A[_[_]], B[_[_]], C[_[_]]] = JoinKind[A, B] { type Joined[F[_]] = C[F] }
    type Inner[A[_[_]], B[_[_]]] = Aux[A, B, Tuple2KK[A, B, ?[_]]]
    type Left[A[_[_]], B[_[_]]] = Aux[A, B, Tuple2KK[A, OptionTK[B, ?[_]], ?[_]]]

    def left[A[_[_]], B[_[_]]: FunctorK]: Left[A, B] = new JoinKind[A, B] {
      type Joined[F[_]] = Tuple2KK[A, OptionTK[B, ?[_]], F]
      def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference] = Tuple2KK(a, OptionTK.liftK(b))
      val kind: String = "left join"
    }

    def inner[A[_[_]], B[_[_]]]: Inner[A, B] = new JoinKind[A, B] {
      type Joined[F[_]] = Tuple2KK[A, B, F]

      def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference] = Tuple2KK(a, b)
      val kind: String = "inner join"
    }

  }

  private def setScope(scope: String): Reference ~> Reference = Reference.mapData {
    λ[ReferenceData ~> ReferenceData] {
      case ReferenceData.Column(n, None) =>
        ReferenceData.Column(n, Some(scope))
      case c @ ReferenceData.Column(_, Some(_)) =>
        //todo this case is impossible, we should have that in the types
        //or else inline it with the catch-all below
        c
      case c => c
    }
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
    final case class Optional[Type](underlying: Reference[Type]) extends Reference[Option[Type]]
    final case class Single[Type](data: ReferenceData[Type], getType: Get[Type]) extends Reference[Type]
    final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
    final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

    val liftOption: Reference ~> λ[a => Reference[Option[a]]] = λ[Reference ~> λ[a => Reference[Option[a]]]] {
      Reference.Optional(_)
    }

    def lift[Type: Get](value: Type)(implicit param: Param[Type HCons HNil]): Reference[Type] =
      Reference.Single[Type](ReferenceData.Lift(value, param), Get[Type])

    def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
      λ[Reference ~> Reference] {
        case Optional(underlying)  => Optional(mapData(fk)(underlying))
        case Single(data, getType) => Single(fk(data), getType)
        case Map(underlying, f)    => Map(mapData(fk)(underlying), f)
        case Product(left, right) =>
          Product(mapData(fk)(left), mapData(fk)(right))
      }

    implicit val apply: Apply[Reference] = new Apply[Reference] {
      def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)

      def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] =
        product(ff, fa).map { case (f, a) => f(a) }
      override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] =
        Reference.Product(fa, fb)
    }
  }

  final case class Query[A[_[_]], Queried](
    base: TableQuery[A],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) {

    def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
      copy(filters = filters.append(filter))

    def compileSql: Query0[Queried] = {
      val (compiledFrom, compiledReference) = base.compileAsFrom

      val (compiledSelectionFrag, getOrRead) = selection(compiledReference).compile.compile

      val frag = fr"select" ++ compiledSelectionFrag ++
        fr"from" ++ compiledFrom ++
        Fragments.whereAnd(
          filters.map(_.apply(compiledReference).compile.compile._1).toList: _*
        )

      implicit val read: Read[Queried] = getOrRead.fold(Read.fromGet(_), identity)

      frag.query[Queried]
    }
  }

  trait ReferenceCompiler {
    def compileReference: Reference ~> TypedFragment
  }

  sealed trait TypedFragment[Type] extends Product with Serializable {
    def optional: TypedFragment[Option[Type]] = TypedFragment.Optional(this)
    def map[B](f: Type => B): TypedFragment[B] = TypedFragment.Map(this, f)
    def product[B](another: TypedFragment[B]): TypedFragment[(Type, B)] = TypedFragment.Product(this, another)

    def compile: (Fragment, Either[Get[Type], Read[Type]]) = TypedFragment.toFragAndGet(this)
  }

  object TypedFragment {

    def toFragAndGet[Type]: TypedFragment[Type] => (Fragment, Either[Get[Type], Read[Type]]) = {
      case Single(frag, get) => (frag, get.asLeft)
      case Optional(underlying) =>
        toFragAndGet(underlying).map {
          case Left(get)   => Read.fromGetOption(get).asRight
          case Right(read) => read.map(_.some).asRight
        }
      case p: Product[a, b] =>
        val (leftFrag, leftGet) = toFragAndGet(p.l)
        val (rightFrag, rightGet) = toFragAndGet(p.r)

        def toRead[A]: Either[Get[A], Read[A]] => Read[A] = _.fold(Read.fromGet(_), identity)

        val combinedGets: Read[(a, b)] = (leftGet, rightGet).bimap(toRead, toRead).tupled

        (leftFrag ++ fr", " ++ rightFrag, combinedGets.asRight)
      case Map(underlying, f) => toFragAndGet(underlying).map(_.bimap(_.map(f), _.map(f)))
    }
    final case class Single[Type](frag: Fragment, get: Get[Type]) extends TypedFragment[Type]
    final case class Optional[Type](underlying: TypedFragment[Type]) extends TypedFragment[Option[Type]]
    final case class Map[A, B](underlying: TypedFragment[A], f: A => B) extends TypedFragment[B]
    final case class Product[A, B](l: TypedFragment[A], r: TypedFragment[B]) extends TypedFragment[(A, B)]
  }

  object ReferenceCompiler {

    val default: ReferenceCompiler = new ReferenceCompiler {
      private def compileData[Type](getType: Get[Type]): ReferenceData[Type] => TypedFragment[Type] = {
        case ReferenceData.Column(column, scope) =>
          val scopeString = scope.foldMap(_ + ".")
          TypedFragment.Single[Type](
            Fragment.const(scopeString + "\"" + column.name + "\""),
            getType
          )
        case l: ReferenceData.Lift[a] =>
          implicit val param: Param[a HCons HNil] = l.into
          val _ = param //to make scalac happy
          TypedFragment.Single[Type](fr"${l.value}", getType)
        case r: ReferenceData.Raw[a] =>
          TypedFragment.Single[Type](r.fragment, getType)
      }

      val compileReference: Reference ~> TypedFragment = λ[Reference ~> TypedFragment] {
        case r: Reference.Optional[a]        => compileReference(r.underlying).optional
        case Reference.Single(data, getType) => compileData(getType)(data)
        case m: Reference.Map[a, b]          => compileReference(m.underlying).map(m.f)
        case p: Reference.Product[a, b]      => compileReference(p.left) product compileReference(p.right)
      }
    }
  }

  //A *->*->* -kinded option transformer
  case class OptionTK[F[_[_]], G[_]](underlying: F[OptionT[G, ?]])

  object OptionTK {

    def liftK[F[_[_]]: FunctorK](fg: F[Reference]): OptionTK[F, Reference] = OptionTK(
      fg.mapK(λ[Reference ~> OptionT[Reference, ?]](g => OptionT(Reference.liftOption(g))))
    )
  }

  //A tuple2 of even-higher-kinded types.
  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
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
        ReferenceData.Raw(a.compile.compile._1 ++ fr"is not null"),
        Get[Boolean]
      )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.compile._1, r.compile.compile._1)),
      Get[Boolean]
    )
}
