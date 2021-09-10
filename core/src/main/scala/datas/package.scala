import cats.~>

import cats.mtl.Stateful
import cats.Apply
import cats.implicits._

package object datas {

  private[datas] type IndexState[F[_]] = Stateful[F, Int]

  private[datas] object IndexState {
    def apply[F[_]](implicit F: IndexState[F]): IndexState[F] = F
    def newIndex[F[_]: IndexState: Apply]: F[Int] = IndexState[F].get <* IndexState[F].modify(_ + 1)
  }

  private[datas] def setScope(scope: String): Reference ~> Reference = Reference.mapData {
    λ[ReferenceData ~> ReferenceData] {
      case ReferenceData.Column(n, None)        =>
        ReferenceData.Column(n, Some(scope))
      case c @ ReferenceData.Column(_, Some(_)) =>
        //todo this case is impossible, we should have that in the types
        //or else inline it with the catch-all below
        c
      case c                                    => c
    }
  }

}
