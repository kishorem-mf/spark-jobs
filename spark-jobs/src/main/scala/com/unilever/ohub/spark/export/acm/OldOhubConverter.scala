package com.unilever.ohub.spark.export.acm

import java.util.regex.Pattern

import com.unilever.ohub.spark.export.TransformationFunction

abstract class OldOhubConverter(sourceIds: Map[String, Int]) extends TransformationFunction[String]{

  val CONCAT_PATTERN = Pattern.compile("^(.+?)~(.+?)~(.+)$")

  def partyTypeId(): String

  def convert(value: String) = impl(value) // TODO remove this overload

  final def impl(value: String): String = {
    val matcher = CONCAT_PATTERN.matcher(value);
    if (matcher.matches()) {
      var sourceId = sourceIds.getOrElse(matcher.group(2), "");
      return matcher.group(1) + "~" + matcher.group(3) + "~" + partyTypeId() + "~" + sourceId;
    } else {
      return value;
    }
  }

  val description = "Converts new concatId to the old OHUB_1.0 representation(countryCode~sourceEntityId~partyTypeId~sourceId). F.e. AU~WUFOO~AB123 -> AU~AB123~3~19"
}

class OperatorOldOhubConverter(sourceIds: Map[String, Int]) extends OldOhubConverter(sourceIds: Map[String, Int]) {
  override def partyTypeId(): String = "1"
}

class ConcatPersonOldOhubConverter(sourceIds: Map[String, Int]) extends OldOhubConverter(sourceIds: Map[String, Int]) {
  override def partyTypeId(): String = "3"
}
