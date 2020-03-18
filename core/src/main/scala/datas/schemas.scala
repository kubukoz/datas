package datas

import cats.implicits._
import doobie._
import cats.~>
import cats.tagless.implicits._
import datas.tagless.InvariantTraverseK
import datas.QueryBase.TableName

object schemas {

  def column[Type: Get: Put](name: String): ColumnK[Type] =
    ColumnK.Named(Column(name), new Meta(Get[Type], Put[Type]))

  def caseClassSchema[F[_[_]]: InvariantTraverseK](name: String, columns: F[ColumnK]): QueryBase[F] =
    QueryBase.FromTable(TableName(name), columns.mapK(columnKToReference), InvariantTraverseK[F])

  private val columnKToReference: ColumnK ~> Reference = λ[ColumnK ~> Reference] {
    case ColumnK.Named(name, meta) =>
      Reference.Single(ReferenceData.Column(name, none), meta)

    case ColumnK.Optional(underlying) =>
      Reference.liftOption(columnKToReference(underlying))
  }

  //todo naming
  sealed trait ColumnK[A] extends Product with Serializable {
    def optional: ColumnK[Option[A]] = ColumnK.Optional(this)
  }

  object ColumnK {
    final case class Named[A](name: Column, meta: Meta[A]) extends ColumnK[A]
    final case class Optional[A](underlying: ColumnK[A]) extends ColumnK[Option[A]]
  }

  final case class Column(name: String) extends AnyVal {
    def showQuoted: String = "\"" + name + "\""
  }

}
