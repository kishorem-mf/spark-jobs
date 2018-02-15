package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.{ GoldenContactPersonRecord, GoldenOperatorRecord, OrderRecord }
import com.unilever.ohub.spark.generic.StringFunctions
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderMerging extends SparkJob {
  private case class OHubIdRefIdAndCountry(ohubId: String, refId: String, countryCode: Option[String])

  def transform(
    spark: SparkSession,
    orderRecords: Dataset[OrderRecord],
    operators: Dataset[OHubIdRefIdAndCountry],
    contactPersons: Dataset[OHubIdRefIdAndCountry]
  ): Dataset[OrderRecord] = {
    import spark.implicits._

    val orders = orderRecords
      .map(order => {
        val operatorRef = order.refOperatorId.map { refOperatorId =>
          StringFunctions.createConcatId(order.countryCode, order.source, refOperatorId)
        }
        val contactRef = order.refContactPersonId.map { refContactPersonId =>
          StringFunctions.createConcatId(order.countryCode, order.source, refContactPersonId)
        }
        order.copy(
          refOperatorId = operatorRef,
          refContactPersonId = contactRef
        )
      })

    val operatorsJoined = orders
      .joinWith(
        operators,
        operators("countryCode") === orders("countryCode")
          and operators("refId").contains(orders("refOperatorId")),
        JoinType.Left
      )
      .map {
        case (order, maybeOperator) => Option(maybeOperator).fold(order) { operator =>
          order.copy(refOperatorId = order.refOperatorId.map(_ => operator.ohubId))
        }
      }

    operatorsJoined
      .joinWith(
        contactPersons,
        contactPersons("countryCode") === operatorsJoined("countryCode")
          and contactPersons("refId").contains(operatorsJoined("refContactPersonId")),
        JoinType.Left
      )
      .map {
        case (order, maybeContactPerson) => Option(maybeContactPerson).fold(order) { contactPerson =>
          order.copy(
            refContactPersonId = order.refContactPersonId.map(_ => contactPerson.ohubId)
          )
        }
      }
  }

  override val neededFilePaths = Array(
    "CONTACT_PERSON_MERGING_INPUT_FILE",
    "OPERATOR_MERGING_INPUT_FILE",
    "ORDER_INPUT_FILE",
    "OUTPUT_FILE"
  )

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (
      contactPersonMergingInputFile: String,
      operatorMergingInputFile: String,
      orderInputFile: String,
      outputFile: String
    ) = filePaths

    log.info(
      s"Merging orders from [$contactPersonMergingInputFile], [$operatorMergingInputFile] " +
        s"and [$orderInputFile] to [$outputFile]"
    )

    val orders = storage
      .readFromParquet[OrderRecord](orderInputFile)

    val operators = storage
      .readFromParquet[GoldenOperatorRecord](operatorMergingInputFile)
      .flatMap { operator =>
        operator.refIds.map { refId =>
          OHubIdRefIdAndCountry(operator.ohubOperatorId, refId, operator.countryCode)
        }
      }

    val contactPersons = storage
      .readFromParquet[GoldenContactPersonRecord](contactPersonMergingInputFile)
      .flatMap { contactPerson =>
        contactPerson.refIds.map { refId =>
          OHubIdRefIdAndCountry(contactPerson.ohubContactPersonId, refId, contactPerson.countryCode)
        }
      }

    val transformed = transform(spark, orders, operators, contactPersons)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
