package com.unilever.ohub.spark.export

import java.sql.Timestamp

import com.unilever.ohub.spark.DomainDataProvider

trait TransformationFunction[T] {
  def impl(input: T): Any

  val exampleValue: String = ""
  val description: String
}

object BooleanTo10Converter extends TransformationFunction[Boolean] {
  def impl(bool: Boolean): String = if (bool) "1" else "0"

  override val exampleValue: String = "1"
  val description: String = "Converts the value to 1 or 0. f.e. true will become \"1\""
}

object BooleanToYNConverter extends TransformationFunction[Boolean] {
  def impl(bool: Boolean): String = if (bool) "Y" else "N"

  override val exampleValue: String = "Y"
  val description: String = "Transforms a boolean to Y(es) or N(o)"
}

object BooleanToYNUConverter extends TransformationFunction[Option[Boolean]] {
  def impl(opt: Option[Boolean]): String = opt.fold("U")(b ⇒ if (b) "Y" else "N")

  override val exampleValue: String = "Y"
  val description: String = "Transforms a boolean to Y(es), N(o) or U(nspecified)"
}

object InvertedBooleanToYNConverter extends TransformationFunction[Boolean] {
  def impl(bool: Boolean): String = if (bool) "N" else "Y"

  override val exampleValue: String = "Y"
  val description: String = "Inverts the value and converts it to Y(es) or N(o). f.e. true will become \"N\""
}

object CleanString extends TransformationFunction[String] {
  def impl(str: String): String = str.replaceAll("[\u0021\u0023\u0025\u0026\u0028\u0029\u002A\u002B\u002D\u002F\u003A\u003B" +
    "\u003C\u003D\u003E\u003F\u0040\u005E\u007C\u007E\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0\u00B1\u00B2\u00B3\u00B6" +
    "\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A\u2543" +
    "\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F\u007B" +
    "\u007D\u00AE\u00F7\u1BFC\u1BFD\u2260\u2264\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B\uEEA3" +
    "\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D˱˳˵˶˹˻˼˽]+", "").trim

  val description: String = "Removes characters that are not always supported by systems (f.e. ⓒ╠☣☺♡♥♪❤\uE2B1) and removes leading and trailing spaces"
}

class ClearInvalidEmail(emailValid: Option[Boolean]) extends TransformationFunction[String] {
  def impl(input: String): String =
    emailValid match {
      case Some(false) => ""
      case _ => input
    }

  override val description: String = "Clears the email address when it is not valid (configured in OHUB)"

}

/*This is exclusively used for ACM Operator and Contactperson exporter.
  This method currently fetches targetOhubId from additionalFields map
  If in case if we want to fetch other values from additionalFields then we need to instantiate (class)
 */
object GetTargetOhubId extends TransformationFunction[Map[String, String]] {
  def impl(input: Map[String, String]): String =
    input.contains("targetOhubId") match {
      case true => input("targetOhubId")
      case _ => ""
    }

  override val description: String = "retrieves targetOhubId value from additional fields"
}

object WeeksClosedToOpened extends TransformationFunction[Int] {
  def impl(weeksClosed: Int): String = if (52 - weeksClosed < 0) 0.toString else (52 - weeksClosed).toString

  val description: String = "Converts weeksClosed to weeksOpened (disregarding years with 52+ weeks). F.e. 12 closed result in 40 opened"
}

class DateUpdatedOrCreated(dateUpdated: Option[Timestamp], dateCreated: Option[Timestamp]) extends TransformationFunction[Option[Timestamp]] {
  override val description: String = "Gets the dateUpdated, or when not supplied, takes the dateUpdated"

  override def impl(input: Option[Timestamp]): Any = {
    dateUpdated.orElse(dateCreated).getOrElse(None)
  }
}

object FormatSourceIDsConverter extends TransformationFunction[String] {
  def impl(sourceNames: String): String = {
    val separator = "-"
    val sourcesMap = DomainDataProvider().sourceIds
    val sourceIdArr = sourceNames.split(",").flatMap(sourcesMap.get).mkString("-")
    if (sourceIdArr.isEmpty) "" else s"$separator" + sourceIdArr + s"$separator"
  }

  override val exampleValue: String = "-3-2-1-"
  val description: String = "Converts sourceNames to sourceIds seperated by hyphen"
}
