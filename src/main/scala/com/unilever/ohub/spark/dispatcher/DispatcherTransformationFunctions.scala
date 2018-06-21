package com.unilever.ohub.spark.dispatcher

import java.sql.Timestamp
import java.time.format.DateTimeFormatter

trait DispatcherTransformationFunctions {

  final val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  final val YES = "Y"

  final val NO = "N"

  final val UNKNOWN = "U"

  final val BOOL_AS_STRING = (bool: Boolean) ⇒ if (bool) YES else NO

  /**
   * Formats the `java.sql.Timestamp` with a date pattern. If no date pattern is given, the default
   * date pattern of 'yyyy-MM-dd HH:mm:ss' is used. Note that the method can throw a [[java.time.DateTimeException]]
   * @param dateTimePattern The date pattern to use, defaults to 'yyyy-MM-dd HH:mm:ss'
   * @param input `java.sql.Timestamp` to format
   * @return String representation of the date format.
   */
  def formatWithPattern(dateTimePattern: String = DATE_FORMAT)(input: Timestamp): String = {
    val pattern = DateTimeFormatter.ofPattern(dateTimePattern)
    pattern.format(input.toLocalDateTime)
  }

  implicit class OptBooleanOps(that: Option[Boolean]) {
    /**
     * Converts an Option[Boolean] to U, Y, N according to the following rules:
     * <ul>
     *   <li>None => Option("U")</li>
     *   <li>Some(true) => Option("Y")</li>
     *   <li>Some(false) => Option("N")</li>
     * </ul>
     * @return Option[String] of the converted String
     */
    def mapToUNYOpt: Option[String] = {
      that.map(BOOL_AS_STRING).orElse(Option(UNKNOWN))
    }

    /**
     * Converts an Option[Boolean] to Y, N according to the following rules:
     * <ul>
     *   <li>None => None</li>
     *   <li>Option(true) => Some("Y")</li>
     *   <li>Option(false) => Some("N")</li>
     * </ul>
     * @return Option[String] of the converted String
     */
    def mapToYNOpt: Option[String] = that.map(BOOL_AS_STRING)
  }

  implicit class BooleanOps(that: Boolean) {
    /**
     * Converts a Boolean to Y, N according to the following rules:
     * <ul>
     *   <li>true => Y</li>
     *   <li>false => N</li>
     * </ul>
     * @return The converted String
     */
    def mapToYN: String = BOOL_AS_STRING(that)

    /**
     * Invert the Boolean
     */
    def invert: Boolean = !that

    /**
     * Converts a Boolean to Y, N according to the following rules
     * <ul>
     *   <li>true => Some("Y")</li>
     *   <li>false => Some("N")</li>
     * </ul>
     * @return Option[String] of the converted String
     */
    def mapToYNOpt: Option[String] = Option(BOOL_AS_STRING(that))
  }

  implicit class OptTimestampOps(that: Option[Timestamp]) {
    /**
     * Converts an Option[java.sql.Timestamp] to a String representation using the
     * following format: 'yyyy-MM-dd HH:mm:ss' according to the following rules:
     * <ul>
     *   <li>Some(Timestamp) => Some('yyyy-MM-dd HH:mm:ss')</li>
     *   <li>None => None</li>
     * </ul>
     * @return Option[String] representation of the default date format
     */
    def mapWithDefaultPatternOpt: Option[String] = that.map(formatWithPattern())
  }

  implicit class TimestampOps(that: Timestamp) {
    /**
     * Converts a `java.sql.Timestamp` to a String representation
     * using the following format: 'yyyy-MM-dd HH:mm:ss'
     * @return String representation of the default date format
     */
    def mapWithDefaultPattern: String = formatWithPattern()(that)

    /**
     * Converts a `java.sql.Timestamp` to a String representation
     * using the following format: 'yyyy-MM-dd HH:mm:ss'
     * @return Option[String] representation of the default date format
     */
    def mapWithDefaultPatternOpt: Option[String] = Option(mapWithDefaultPattern)
  }
}
