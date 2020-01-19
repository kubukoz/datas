package datas

import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import doobie.implicits._
import cats.data.Chain
import cats.tagless.FunctorK
import cats.tagless.implicits._
import cats.mtl.instances.all._
import cats.FlatMap
import datas.tagless.TraverseK
import datas.schemas.TableName

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
    how: JoinKind.type => JoinKind[A, B, Joined]
  )(
    onClause: (A[Reference], B[Reference]) => Reference[Boolean]
  ): QueryBase[Joined] =
    QueryBase.Join(this, another, how(JoinKind), onClause)

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
      IndexState.newIndex[F].map { index =>
        val scope = t.table.indexed(index).name
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

          (joinFrag, kind.buildJoint(leftCompiledReference, rightCompiledReference))
      }
  }

}
