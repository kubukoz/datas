package datas

import cats.data.Chain
import doobie.util.query.Query0
import cats.data.State
import doobie.implicits._
import cats.mtl.instances.all._
import doobie.util.Read
import doobie.Fragments

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
