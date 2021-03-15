package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Asset
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AssetEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object AssetConverter extends CommonDomainGateKeeper[Asset] with AssetEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Asset = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Asset(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubId = Option.empty,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,

        crmId = optional("crmId"),
        name = optional("name"),
        `type` = optional("type"),
        brandName = optional("brandName"),
        description = optional("description"),
        dimensions = optional("dimensions"),
        numberOfTimesRepaired = optional("numberOfTimesRepaired",toInt),
        powerConsumption = optional("powerConsumption"),
        numberOfCabinetBaskets = optional("numberOfCabinetBaskets",toInt),
        numberOfCabinetFacings = optional("numberOfCabinetFacings",toInt),
        serialNumber = optional("serialNumber"),
        oohClassification = optional("oohClassification"),
        lifecyclePhase = optional("lifecyclePhase"),
        capacityInLiters = optional("capacityInLiters",toBigDecimal),
        dateCreatedInUdl = optional("dateCreatedInUdl",parseDateTimeUnsafe()),
        leasedOrSold = optional("leasedOrSold"),
        crmTaskId = optional("crmTaskId"),
        assemblyDate = optional("assemblyDate",parseDateTimeUnsafe()),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
