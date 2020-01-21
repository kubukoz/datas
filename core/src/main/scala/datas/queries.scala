package datas

import cats.data.Chain
import doobie.util.query.Query0
import cats.data.State
import doobie.implicits._
import cats.mtl.instances.all._
import cats.implicits._
import doobie.util.Read
import doobie.Fragments

sealed trait Query[A[_[_]], Queried] extends Product with Serializable {
  def where(filter: A[Reference] => Reference[Boolean]): Query[A, Queried]

  def compileSql: Query0[Queried]
}

private[datas] object Query {
  final case class Function[A[_[_]], Queried](
    base: QueryBase[A],
    selection: A[Reference] => Reference[Queried],
    filters: Chain[A[Reference] => Reference[Boolean]]
  ) extends Query[A, Queried] {

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

  final case class All[A[_[_]]](base: QueryBase[A], filters: Chain[A[Reference] => Reference[Boolean]]) extends Query[A, A[cats.Id]] {
    //todo factor out to common class
    def where(filter: A[Reference] => Reference[Boolean]): Query[A, A[cats.Id]] = copy(filters = filters.append(filter))

    def compileSql: Query0[A[cats.Id]] = base match {
      case ft: QueryBase.FromTable[A] =>
        import ops._
        base
          .select(r => ft.traverseK.sequenceKId(r))
          // this has got to be the ugliest thing, but it's just for now, I swear...
          .where(filters.foldLeft((_: A[Reference]) => Reference.lift(false))((_, _).mapN(_ || _)))
          .compileSql
      case _: QueryBase.Join[a, b, A] => ???
    }
  }
}
