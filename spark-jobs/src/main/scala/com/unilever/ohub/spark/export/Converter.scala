package com.unilever.ohub.spark.export

import com.unilever.ohub.spark.domain.DomainEntity

import scala.reflect.runtime.universe._

trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] extends MappingExplanation[DomainType] {

  def convert(implicit d: DomainType, explain: Boolean = false): OutboundType

  def getValue[T: TypeTag](name: String, transformFunction: TransformationFunction[T])(implicit input: DomainType, explain: Boolean): AnyRef = getValue(name, Some(transformFunction))

  def getValue[T: TypeTag](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean): AnyRef = {
    val field = input.getClass.getDeclaredField(name)
    field.setAccessible(true)
    val value = field.get(input)

    if (explain) {
      getExplanation[T](name, transformFunction)
    } else {
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
