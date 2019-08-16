package com.unilever.ohub.spark.export

import com.unilever.ohub.spark.domain.DomainEntity
import org.apache.log4j.{LogManager, Logger}
import org.codehaus.jackson.map.ObjectMapper

import scala.reflect.runtime.universe._


trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] extends TypeConversionFunctions {

  private val log: Logger = LogManager.getLogger(getClass)

  def convert(implicit d: DomainType, explain: Boolean = false): OutboundType

  def getValue[T: TypeTag](name: String, transformFunction: TransformationFunction[T])(implicit input: DomainType, explain: Boolean): AnyRef = getValue(name, Some(transformFunction))

  def getValue[T: TypeTag](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean): AnyRef = {
    val field = input.getClass.getDeclaredField(name)
    if (explain) {
      val mappingInfo = new ObjectMapper().createObjectNode()

      mappingInfo.put("fromEntity", input.getClass.getSimpleName)
      mappingInfo.put("fromFieldName", name)
      mappingInfo.put("fromFieldType", field.getGenericType.getTypeName)

      val transform =
        if (transformFunction.isDefined) {
          val transformInfo = mappingInfo.objectNode()
          transformInfo.put("function", transformFunction.get.getClass.getSimpleName)
          transformInfo.put("description", transformFunction.get.description)
          mappingInfo.put("transform", transformInfo)
        }

      mappingInfo.toString
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
