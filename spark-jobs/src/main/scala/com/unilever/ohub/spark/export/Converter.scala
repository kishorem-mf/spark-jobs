package com.unilever.ohub.spark.export

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

import com.unilever.ohub.spark.domain.DomainEntity
import org.codehaus.jackson.map.ObjectMapper

import scala.reflect.runtime.universe._

trait Converter[DomainType <: DomainEntity, OutboundType <: OutboundEntity] extends TypeConversionFunctions {

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

  private def getExplanation[T: TypeTag](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean): AnyRef = {
    val genericDataTypePattern = "(scala.Option<)?([a-z]+\\.)+([A-Z][A-Za-z]+)>?".r
    def getDataType(genericType: String) = genericDataTypePattern.replaceFirstIn(genericType, "$3")
    def dataTypeIsOptional(genericType: String) = genericDataTypePattern.replaceFirstIn(genericType, "$1").length > 0

    val field = input.getClass.getDeclaredField(name)

    val mappingInfo = new ObjectMapper().createObjectNode()

    mappingInfo.put("fromEntity", input.getClass.getSimpleName)
    mappingInfo.put("fromFieldName", name)
    val dataType = getDataType(field.getGenericType.getTypeName)
    mappingInfo.put("fromFieldType", dataType)

    val fieldIsOptional = dataTypeIsOptional(field.getGenericType.getTypeName)
    mappingInfo.put("fromFieldOptional", fieldIsOptional)

    if (transformFunction.isDefined) {
      val transformInfo = mappingInfo.objectNode()
      transformInfo.put("function", transformFunction.get.getClass.getSimpleName)
      transformInfo.put("description", transformFunction.get.description)
      mappingInfo.put("transform", transformInfo)
    } else {
      dataType match {
        case "Timestamp" => {
          mappingInfo.put("datePattern", timestampPattern)
          mappingInfo.put("exampleDate", DateTimeFormatter.ofPattern(timestampPattern).format(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)))
        }
        case "Date" => {
          mappingInfo.put("datePattern", datePattern)
          mappingInfo.put("exampleDate", DateTimeFormatter.ofPattern(datePattern).format(LocalDate.now()))
        }
        case _ =>
      }
    }
    mappingInfo.toString
  }
}
