package com.unilever.ohub.spark.export

trait TransformationFunctions {

  val booleanTo10Converter = (bool: Boolean) ⇒ if (bool) "1" else "0"
  val bigDecimalTo2Decimals = (b: BigDecimal) ⇒ b.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  val cleanString = (str: String) ⇒ str.replaceAll("[\u0021\u0023\u0025\u0026\u0028\u0029\u002A\u002B\u002D\u002F\u003A\u003B\u003C\u003D\u003E\u003F\u0040\u005E\u007C\u007E\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0\u00B1\u00B2\u00B3\u00B6\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A\u2543\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F\u007B\u007D\u00AE\u00F7\u1BFC\u1BFD\u2260\u2264\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B\uEEA3\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D˱˳˵˶˹˻˼˽]+", "").trim

  protected[export] implicit def optionalStringToString(x: Option[String]): String = x.getOrElse("")

  protected[export] implicit def optionalIntToString(x: Option[Int]): String = x.map(_.toString).getOrElse("")

  protected[export] implicit def optionalDoubleToString(x: Option[Double]): String = x.map(_.formatted("%.2f")).getOrElse("")

  protected[export] implicit def doubleToString(d: Double): String = d.formatted("%.2f")

  protected[export] implicit def bigDecimalToString(bigDecimal: BigDecimal): String = bigDecimalTo2Decimals(bigDecimal)

  protected[export] implicit def optionalBigDecimalToString(input: Option[BigDecimal]): String = input.map(bigDecimalTo2Decimals)

  protected[export] implicit def booleanToYNUConverter(input: Option[Boolean]): String = if (input.isEmpty) Some("U") else input.map(b ⇒ if (b) "Y" else "N")

  protected[export] implicit def booleanToYNConverter(input: Boolean): String = if (input) "Y" else "N"

}