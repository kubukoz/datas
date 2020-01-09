package datas

import cats.tagless.FunctorK
import cats.Apply
import cats.arrow.FunctionK
import cats.~>
import cats.data.OptionT
import cats.tagless.implicits._

object tagless {

  trait TraverseK[Alg[_[_]]] extends FunctorK[Alg] {

    def traverseK[F[_], G[_]: Apply, H[_]](alg: Alg[F])(fk: F ~> λ[a => G[H[a]]]): G[Alg[H]]

    def sequenceK[F[_]: Apply, G[_]](alg: Alg[λ[a => F[G[a]]]]): F[Alg[G]] =
      traverseK[λ[a => F[G[a]]], F, G](alg)(FunctionK.id[λ[a => F[G[a]]]])

    def mapK[F[_], G[_]](af: Alg[F])(fk: F ~> G): Alg[G] = traverseK[F, cats.Id, G](af)(fk)

    def sequenceKId[F[_]: Apply](alg: Alg[F]): F[Alg[cats.Id]] = sequenceK[F, cats.Id](alg)
  }

  object TraverseK {
    def apply[Alg[_[_]]](implicit S: TraverseK[Alg]): TraverseK[Alg] = S
  }

//A ((*->*)->*)->* (or so)-kinded option transformer
  case class OptionTK[F[_[_]], G[_]](underlying: F[OptionT[G, ?]])

  object OptionTK {
    def liftK[F[_[_]]: FunctorK, G[_]](fg: F[G])(lift: G ~> OptionT[G, ?]): OptionTK[F, G] = OptionTK(fg.mapK(lift))
  }

//A tuple2 of even-higher-kinded types.
  final case class Tuple2KK[A[_[_]], B[_[_]], F[_]](left: A[F], right: B[F]) {
    def asTuple: (A[F], B[F]) = (left, right)
  }
}
