package datas

import datas.tagless.Tuple2KK
import datas.tagless.OptionTK
import cats.tagless.FunctorK

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
