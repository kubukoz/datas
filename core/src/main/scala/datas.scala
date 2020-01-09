package datas

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
import cats.mtl.instances.all._
import cats.FlatMap
import cats.data.OptionT
import datas.tagless.TraverseK
import datas.tagless.Tuple2KK
import datas.tagless.OptionTK

//todo naming
sealed trait ColumnK[A] extends Product with Serializable {
  def optional: ColumnK[Option[A]] = ColumnK.Optional(this)
}

object ColumnK {
  final case class Named[A](name: Column, get: Get[A]) extends ColumnK[A]
  final case class Optional[A](underlying: ColumnK[A]) extends ColumnK[Option[A]]
}

object schemas {
  private val columnToStRef: ColumnK ~> Reference = λ[ColumnK ~> Reference] {
    case ColumnK.Named(name, get) =>
      Reference.Single(ReferenceData.Column(name, none), get)

    case ColumnK.Optional(underlying) =>
      Reference.liftOption(columnToStRef(underlying))
  }

  def column[Type: Get](name: String): ColumnK[Type] =
    ColumnK.Named(Column(name), Get[Type])

  def caseClassSchema[F[_[_]]: TraverseK](name: TableName, columns: F[ColumnK]): QueryBase[F] =
    QueryBase.FromTable(name, columns.mapK(columnToStRef), TraverseK[F])
}

final case class TableName(name: String) extends AnyVal {
  private[datas] def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
}

/**
  * QueryBase: a thing you can query from. It'll usually be a table or a join thereof.
  */
sealed trait QueryBase[A[_[_]]] extends Product with Serializable {

  def innerJoin[B[_[_]]](
    another: QueryBase[B]
  )(
    onClause: (A[Reference], B[Reference]) => Reference[Boolean]
  ): QueryBase[JoinKind.Inner[A, B]#Out] =
    join(another)(_.inner)(onClause)

  def leftJoin[B[_[_]]: FunctorK](
    another: QueryBase[B]
  )(
    onClause: (A[Reference], B[Reference]) => Reference[Boolean]
  ): QueryBase[JoinKind.Left[A, B]#Out] =
    join(another)(_.left)(onClause)

  private def join[B[_[_]], Joined[_[_]]](
    another: QueryBase[B]
  )(
    kindF: JoinKind.type => JoinKind[A, B, Joined]
  )(
    onClause: (A[Reference], B[Reference]) => Reference[Boolean]
  ): QueryBase[Joined] =
    QueryBase.Join(this, another, kindF(JoinKind), onClause)

  def selectAll: Query[A, A[cats.Id]] = select { aref =>
    this match {
      case ft: QueryBase.FromTable[A] => ft.traverseK.sequenceKId(aref)
      case _                          => throw new Exception("select * isn't supported on joins yet")
    }
  }

  def select[Queried](selection: A[Reference] => Reference[Queried]): Query[A, Queried] =
    Query(this, selection, filters = Chain.empty)
}

private[datas] object QueryBase {
  final case class FromTable[A[_[_]]](table: TableName, lifted: A[Reference], traverseK: TraverseK[A]) extends QueryBase[A]

  final case class Join[A[_[_]], B[_[_]], Joined[_[_]]](
    left: QueryBase[A],
    right: QueryBase[B],
    kind: JoinKind[A, B, Joined],
    onClause: (A[Reference], B[Reference]) => Reference[Boolean]
  ) extends QueryBase[Joined]

  /**
    * Returns: the compiled query base (from + joins) and the scoped references underlying it (passed later to selections and filters).
    */
  def compileQuery[A[_[_]], F[_]: IndexState: FlatMap]: QueryBase[A] => F[(Fragment, A[Reference])] = {
    case t: FromTable[A] =>
      implicit val functorK: FunctorK[A] = t.traverseK
      IndexState.getAndInc[F].map { index =>
        val scope = t.table.name + "_x" + index
        (
          t.table.identifierFragment ++ Fragment.const(scope),
          t.lifted.mapK(setScope(scope))
        )
      }

    case j: Join[a, b, k] =>
      import j._
      (compileQuery[a, F].apply(left), compileQuery[b, F].apply(right)).mapN {
        case ((leftFrag, leftCompiledReference), (rightFrag, rightCompiledReference)) =>
          val thisJoinClause = onClause(leftCompiledReference, rightCompiledReference).compile.frag

          val joinFrag = leftFrag ++ Fragment.const(kind.kind) ++ rightFrag ++ fr"on" ++ thisJoinClause

          (joinFrag, kind.buildJoined(leftCompiledReference, rightCompiledReference))
      }
  }

}

trait JoinKind[A[_[_]], B[_[_]], Joined[_[_]]] {
  final type Out[F[_]] = Joined[F]

  def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference]
  def kind: String
}

object JoinKind {
  type Inner[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, B, ?[_]]]
  type Left[A[_[_]], B[_[_]]] = JoinKind[A, B, Tuple2KK[A, OptionTK[B, ?[_]], ?[_]]]

  def left[A[_[_]], B[_[_]]: FunctorK]: Left[A, B] = make("left join")((a, b) => Tuple2KK(a, OptionTK.liftK(b)(Reference.liftOptionK)))
  def inner[A[_[_]], B[_[_]]]: Inner[A, B] = make("inner join")(Tuple2KK.apply _)

  private def make[A[_[_]], B[_[_]], Joined[_[_]]](
    name: String
  )(
    build: (A[Reference], B[Reference]) => Joined[Reference]
  ): JoinKind[A, B, Joined] = new JoinKind[A, B, Joined] {
    def buildJoined(a: A[Reference], b: B[Reference]): Joined[Reference] = build(a, b)
    val kind: String = name
  }
}

final case class Column(name: String) extends AnyVal {
  def showQuoted: String = "\"" + name + "\""
}

sealed trait ReferenceData[Type] extends Product with Serializable

object ReferenceData {
  final case class Column[A](col: datas.Column, scope: Option[String]) extends ReferenceData[A]
  final case class Lift[Type](value: Type, into: Param[Type HCons HNil]) extends ReferenceData[Type]
  final case class Raw[Type](fragment: Fragment) extends ReferenceData[Type]

  private[datas] def compileData[Type](getType: Get[Type]): ReferenceData[Type] => TypedFragment[Type] = {
    case ReferenceData.Column(column, scope) =>
      val scopeString = scope.foldMap(_ + ".")
      TypedFragment(Fragment.const(scopeString + column.showQuoted), getType)

    case l: ReferenceData.Lift[a] =>
      implicit val param: Param[a HCons HNil] = l.into
      val _ = param //to make scalac happy
      TypedFragment(fr"${l.value}", getType)

    case r: ReferenceData.Raw[a] => TypedFragment(r.fragment, getType)
  }

  final case class TypedFragment[Type](fragment: Fragment, get: Get[Type])
}

sealed trait Reference[Type] extends Product with Serializable {

  private[datas] def compile: CompiledReference[Type] =
    Reference.compileReference(this)
}

object Reference {
  final case class Optional[Type](underlying: Reference[Type]) extends Reference[Option[Type]]
  final case class Single[Type](data: ReferenceData[Type], getType: Get[Type]) extends Reference[Type]
  final case class Product[L, R](left: Reference[L], right: Reference[R]) extends Reference[(L, R)]
  final case class Map[A, B](underlying: Reference[A], f: A => B) extends Reference[B]

  val liftOptionK: Reference ~> OptionT[Reference, ?] = λ[Reference ~> OptionT[Reference, ?]](r => OptionT(Optional(r)))

  def liftOption[Type](reference: Reference[Type]): Reference[Option[Type]] = liftOptionK(reference).value

  def lift[Type: Get](value: Type)(implicit param: Param[Type HCons HNil]): Reference[Type] =
    Reference.Single[Type](ReferenceData.Lift(value, param), Get[Type])

  def mapData(fk: ReferenceData ~> ReferenceData): Reference ~> Reference =
    λ[Reference ~> Reference] {
      case Single(data, getType) => Single(fk(data), getType)
      case Optional(underlying)  => Optional(mapData(fk)(underlying))
      case Map(underlying, f)    => Map(mapData(fk)(underlying), f)
      case Product(left, right)  => Product(mapData(fk)(left), mapData(fk)(right))
    }

  implicit val apply: Apply[Reference] = new Apply[Reference] {
    override def map[A, B](fa: Reference[A])(f: A => B): Reference[B] = Map(fa, f)
    override def ap[A, B](ff: Reference[A => B])(fa: Reference[A]): Reference[B] = product(ff, fa).map { case (f, a) => f(a) }
    override def product[A, B](fa: Reference[A], fb: Reference[B]): Reference[(A, B)] = Reference.Product(fa, fb)
  }

  private[datas] val compileReference: Reference ~> CompiledReference = λ[Reference ~> CompiledReference] {
    case Reference.Single(data, getType) =>
      val tf = ReferenceData.compileData(getType)(data)
      CompiledReference(tf.fragment, tf.get.asLeft)

    case r: Reference.Optional[a] =>
      compileReference(r.underlying).ermap {
        case Left(get)   => Read.fromGetOption(get).asRight
        case Right(read) => read.map(_.some).asRight
      }

    case m: Reference.Map[a, b] => compileReference(m.underlying).map(m.f)

    case p: Reference.Product[a, b] =>
      def toRead[A]: Either[Get[A], Read[A]] => Read[A] = _.fold(Read.fromGet(_), identity)

      val (leftFrag, leftRead) = compileReference(p.left).rmap(toRead)
      val (rightFrag, rightRead) = compileReference(p.right).rmap(toRead)

      implicit val lR = leftRead
      implicit val rR = rightRead

      val _ = (lR, rR) //making scalac happy

      CompiledReference(leftFrag ++ fr", " ++ rightFrag, Read[(a, b)].asRight)
  }
}

final case class Query[A[_[_]], Queried](
  base: QueryBase[A],
  selection: A[Reference] => Reference[Queried],
  filters: Chain[A[Reference] => Reference[Boolean]]
) {

  def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried] =
    copy(filters = filters.append(filter))

  def compileSql: Query0[Queried] = {
    val (compiledQueryBase, compiledReference) = QueryBase.compileQuery[A, State[Int, *]].apply(base).runA(0).value

    val compiledSelection = selection(compiledReference).compile

    val frag = fr"select" ++ compiledSelection.frag ++
      fr"from" ++ compiledQueryBase ++
      Fragments.whereAnd(
        filters.map(_.apply(compiledReference).compile.frag).toList: _*
      )

    implicit val read: Read[Queried] = compiledSelection.readOrGet.fold(Read.fromGet(_), identity)

    frag.query[Queried]
  }
}

private[datas] final case class CompiledReference[Type](frag: Fragment, readOrGet: Either[Get[Type], Read[Type]]) {
  def rmap[T2](f: Either[Get[Type], Read[Type]] => T2): (Fragment, T2) = (frag, f(readOrGet))

  def ermap[T2](f: Either[Get[Type], Read[Type]] => Either[Get[T2], Read[T2]]): CompiledReference[T2] =
    CompiledReference(frag, f(readOrGet))

  def map[T2](f: Type => T2): CompiledReference[T2] = ermap(_.bimap(_.map(f), _.map(f)))
}

object ops {

  def over[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr">" ++ _)

  def equal[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def equalOptionL[Type]: (Reference[Option[Type]], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"=" ++ _)

  def notNull[Type](a: Reference[Option[Type]]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(a.compile.frag ++ fr"is not null"),
      Get[Boolean]
    )

  def nonEqual[Type]: (Reference[Type], Reference[Type]) => Reference[Boolean] =
    binary(_ ++ fr"<>" ++ _)

  def binary[L, R](f: (Fragment, Fragment) => Fragment)(l: Reference[L], r: Reference[R]): Reference[Boolean] =
    Reference.Single(
      ReferenceData.Raw(f(l.compile.frag, r.compile.frag)),
      Get[Boolean]
    )
}
