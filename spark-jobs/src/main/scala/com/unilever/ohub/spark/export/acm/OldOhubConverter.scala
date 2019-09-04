package com.unilever.ohub.spark.export.acm

import java.util.regex.Pattern

abstract class OldOhubConverter(sourceIds: Map[String, Int]) {

  val CONCAT_PATTERN = Pattern.compile("^(.+?)~(.+?)~(.+)$")

  def partyTypeId(): String

  final def convert(value: String): String = {
    val matcher = CONCAT_PATTERN.matcher(value);
    if (matcher.matches()) {
      var sourceId = sourceIds.getOrElse(matcher.group(2), "");
      matcher.group(1) + "~" + matcher.group(3) + "~" + partyTypeId() + "~" + sourceId
    } else {
      value
    }
  }
}

class OperatorOldOhubConverter(sourceIds: Map[String, Int]) extends OldOhubConverter(sourceIds: Map[String, Int]) {
  override def partyTypeId(): String = "1"
}

class ConcatPersonOldOhubConverter(sourceIds: Map[String, Int]) extends OldOhubConverter(sourceIds: Map[String, Int]) {
  override def partyTypeId(): String = "3"
}
