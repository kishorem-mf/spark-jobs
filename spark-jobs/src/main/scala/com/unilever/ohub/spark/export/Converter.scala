package com.unilever.ohub.spark.export

import com.unilever.ohub.spark.domain.DomainEntity

import scala.reflect.runtime.universe._

trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] extends MappingExplanation[DomainType] {

  def convert(implicit d: DomainType, explain: Boolean = false): OutboundType

  def getValue[T: TypeTag](name: String, transformFunction: TransformationFunction[T])(implicit input: DomainType, explain: Boolean): Any = getValue(name, Some(transformFunction))

  /**
    * Function get the value from the implicitly present DomainType. If it's an Option, it's will either be unboxed or, when None, returned as an empty String.
    *
    * If the implicitly present boolean explain is set to true, an explanation is return as value instead of the actual value.
    */
  def getValue[T: TypeTag](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean): Any = {
    val field = input.getClass.getDeclaredField(name)
    field.setAccessible(true)
    val value = field.get(input)

    if (explain) {
      getExplanation[T](name, transformFunction)
    } else {
      if (transformFunction.isDefined) {
        def tryInvocation(value: Any, function: TransformationFunction[T]): Any = {
          try {
            transformFunction.get.impl(value.asInstanceOf[T])
          } catch {
            case _: ClassCastException => {
              value match {
                case None => ""
                case Some(optVal) => tryInvocation(optVal.asInstanceOf[Any], function)
                case _ => "Error in additional fields due to target ohubid"
              }
            }
            case _: NullPointerException => {
              value match {
                case None => ""
                case Some(optVal) => tryInvocation(optVal.asInstanceOf[Any], function)
                case _ => "Error in additional fields due to target ohubid"
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
