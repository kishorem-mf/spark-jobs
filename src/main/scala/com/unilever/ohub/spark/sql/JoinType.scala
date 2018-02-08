package com.unilever.ohub.spark.sql

import scala.language.implicitConversions

sealed trait JoinType { self =>
  private def value: String = self match {
    case Cross => "cross"
    case Full => "full"
    case FullOuter => "full_outer"
    case Inner => "inner"
    case Left => "left"
    case LeftAnti => "left_anti"
    case LeftOuter => "left_outer"
    case LeftSemi => "left_semi"
    case Outer => "outer"
    case Right => "right"
    case RightOuter => "right_outer"
  }
}

object JoinType {
  implicit def asString(joinType: JoinType): String = joinType.value
}

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
