package com.unilever.ohub.spark.export

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}

import com.unilever.ohub.spark.domain.DomainEntity

import scala.reflect.runtime.universe._

case class FieldMapping(fromEntity: String,
                        fromField: String,
                        fromType: String,
                        pattern: String = "",
                        exampleValue: String = "",
                        `type`: String = "String",
                        required: Boolean,
                        transformation: String = "")

trait MappingExplanation[DomainType <: DomainEntity] extends TypeConversionFunctions {
  protected def getExplanation[T](name: String, transformFunction: Option[TransformationFunction[T]] = None)(implicit input: DomainType, explain: Boolean, tag: TypeTag[T]): AnyRef = {
    val genericDataTypePattern = "(scala.Option<)?([a-z]+\\.)+([A-Z][A-Za-z]+)>?".r

    val field = input.getClass.getDeclaredField(name)

    def getDataType(): String = {
      val typeName = genericDataTypePattern.replaceFirstIn(field.getGenericType.getTypeName, "$3") match {
        case "Object" => tag.tpe.toString
        case other => other
      }
      typeName.capitalize
    }

    def isDataTypeRequired(genericType: String) = !genericType.contains("scala.Option<")

    val dataType = getDataType()
    //    val dataType = getDataType(field.getGenericType.getTypeName)

    val fieldMapping = FieldMapping(
      fromEntity = input.getClass.getSimpleName,
      fromField = name,
      fromType = dataType,
      required = isDataTypeRequired(field.getGenericType.getTypeName)
    )

    if (transformFunction.isDefined) {
      fieldMapping.copy(transformation = transformFunction.get.description, exampleValue = transformFunction.get.exampleValue)
    } else {
      dataType match {
        case "Timestamp" => {
          fieldMapping.copy(pattern = timestampPattern, exampleValue = DateTimeFormatter.ofPattern(timestampPattern).format(LocalDateTime.now().truncatedTo(ChronoUnit.HOURS)))
        }
        case "Date" => {
          fieldMapping.copy(pattern = datePattern, exampleValue = DateTimeFormatter.ofPattern(datePattern).format(LocalDate.now()))
        }
        case _ => fieldMapping
      }
    }
  }
}
