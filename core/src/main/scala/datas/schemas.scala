package datas

import cats.implicits._
import doobie.util.fragment.Fragment
import doobie._
import cats.~>
import cats.tagless.implicits._
import datas.tagless.TraverseK

object schemas {

  def column[Type: Get](name: String): ColumnK[Type] =
    ColumnK.Named(Column(name), Get[Type])

  def caseClassSchema[F[_[_]]: TraverseK](name: String, columns: F[ColumnK]): QueryBase[F] =
    QueryBase.FromTable(TableName(name), columns.mapK(columnKToReference), TraverseK[F])

  private val columnKToReference: ColumnK ~> Reference = λ[ColumnK ~> Reference] {
    case ColumnK.Named(name, get) =>
      Reference.Single(ReferenceData.Column(name, none), get)

    case ColumnK.Optional(underlying) =>
      Reference.liftOption(columnKToReference(underlying))
  }

  final case class TableName(name: String) extends AnyVal {
    private[datas] def identifierFragment: Fragment = Fragment.const("\"" + name + "\"")
    private[datas] def indexed(index: Int): TableName = TableName(name + "_x" + index)
  }

  //todo naming
  sealed trait ColumnK[A] extends Product with Serializable {
    def optional: ColumnK[Option[A]] = ColumnK.Optional(this)
  }

  object ColumnK {
    final case class Named[A](name: Column, get: Get[A]) extends ColumnK[A]
    final case class Optional[A](underlying: ColumnK[A]) extends ColumnK[Option[A]]
  }

  final case class Column(name: String) extends AnyVal {
    def showQuoted: String = "\"" + name + "\""
  }

}
