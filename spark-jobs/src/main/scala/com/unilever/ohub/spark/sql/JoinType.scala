package com.unilever.ohub.spark.sql

import scala.language.implicitConversions

sealed trait JoinType {
  self ⇒

  // scalastyle:off
  // cyclomatic complexity to high, but code is easy to read code, so suppressing it
  private def value: String = self match {
    case JoinType.Cross ⇒ "cross"
    case JoinType.Full ⇒ "full"
    case JoinType.FullOuter ⇒ "full_outer"
    case JoinType.Inner ⇒ "inner"
    case JoinType.Left ⇒ "left"
    case JoinType.LeftAnti ⇒ "left_anti"
    case JoinType.LeftOuter ⇒ "left_outer"
    case JoinType.LeftSemi ⇒ "left_semi"
    case JoinType.Outer ⇒ "outer"
    case JoinType.Right ⇒ "right"
    case JoinType.RightOuter ⇒ "right_outer"
  }

  // scalastyle:on
}

object JoinType {
  implicit def asString(joinType: JoinType): String = joinType.value

  object Cross extends JoinType

  object Full extends JoinType

  object FullOuter extends JoinType

  object Inner extends JoinType

  object Left extends JoinType

  object LeftAnti extends JoinType

  object LeftOuter extends JoinType

  object LeftSemi extends JoinType

  object Outer extends JoinType

  object Right extends JoinType

  object RightOuter extends JoinType

}
