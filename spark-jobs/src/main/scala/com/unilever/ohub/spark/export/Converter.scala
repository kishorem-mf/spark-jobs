package com.unilever.ohub.spark.export

import com.unilever.ohub.spark.domain.DomainEntity
import org.apache.log4j.{LogManager, Logger}

import scala.reflect.runtime.universe._


trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] extends TypeConversionFunctions {

  private val log: Logger = LogManager.getLogger(getClass)

  def convert(implicit d: DomainType, explain: Boolean = false): OutboundType

  def getValue[T: TypeTag](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean = false): AnyRef = {
    val field = input.getClass.getDeclaredField(name)
    if (explain) {
      lazy val className = input.getClass.getSimpleName

      val fromEntity = s"From entity: $className\n"
      val fromField = s"From field name: $name\n"
      val fromType = s"From field type: ${field.getGenericType.getTypeName}\n"
      val transform =
        if (transformFunction.isDefined) s"Transforming using ${transformFunction.get.getClass.getSimpleName}, desciption: ${transformFunction.get.description}"
        else ""
      fromEntity + fromField + fromType + transform
    } else {
      field.setAccessible(true)
      val value = field.get(input)
      if (transformFunction.isDefined) {

        def tryInvocation(value: AnyRef, function: TransformationFunction[T]): AnyRef = {
          try {
            transformFunction.get.impl(value.asInstanceOf[T])
          } catch {
            case _: ClassCastException => {
              value match {
                case None => ""
                case Some(optVal) => tryInvocation(optVal.asInstanceOf[AnyRef], function)
              }
            }
          }
        }

        tryInvocation(value, transformFunction.get)
      } else {
        value
      }
    }
  }
}
