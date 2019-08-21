package com.unilever.ohub.spark.export

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

trait TypeConversionFunctions {

  val bigDecimalTo2Decimals = (b: BigDecimal) ⇒ b.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  protected[export] val timestampPattern = "yyyy-MM-dd hh:mm:ss:SSS"
  protected[export] val datePattern = "yyyy-MM-dd"

  protected[export] implicit def anyRefToString(x: AnyRef): String = {
    x match {
      case None => ""
      case value: Timestamp => value
      case Some(value: Timestamp) => value
      case Some(value: Int) => value.toString // TODO remove optional conversions after all converters are using getValue
      case Some(value: Double) => value
      case Some(value: BigDecimal) => value
      case Some(value: Date) => value
      case Some(value: String) => value
      case value: BigDecimal => value
      case value: String => value
      case value: java.lang.Integer => value.toString
      case value: java.lang.Boolean => value.toString
      case value: FieldMapping => mapper.writeValueAsString(value)
      case _ => throw new IllegalArgumentException(s"No explicit cast specified from anyref to ${x.getClass}")
    }
  }

  protected[export] implicit def doubleToString(d: Double): String = d.formatted("%.2f")

  protected[export] implicit def bigDecimalToString(bigDecimal: BigDecimal): String = bigDecimalTo2Decimals(bigDecimal)

  protected[export] implicit def timestampToString(input: Timestamp): String = DateTimeFormatter.ofPattern(timestampPattern).format(input.toLocalDateTime)

  protected[export] implicit def formatDateWithPattern(input: Date): String = DateTimeFormatter.ofPattern(datePattern).format(input.toLocalDate)
