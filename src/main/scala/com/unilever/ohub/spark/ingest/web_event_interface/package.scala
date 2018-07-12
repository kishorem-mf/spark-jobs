package com.unilever.ohub.spark.ingest

import scala.language.implicitConversions

/**
 * web_event_interface contains converters for the data source 'web_event_interface' which generates CSV files. The source identifier
 * is the same as the emakina data source and is 'EMAKINA'.
 */
package object web_event_interface {
  /**
   * Automatically convert a value 'A' into an Option[A]
   */
  implicit def toOptionOfA[A](that: A): Option[A] = Option(that)
}
