package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.AssetMovement
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{AssetMovementEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object AssetMovementConverter extends CommonDomainGateKeeper[AssetMovement] with AssetMovementEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ AssetMovement = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      AssetMovement(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = mandatory("customerType"),
        isActive = mandatory("isActive", toBoolean),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        ohubId = Option.empty,
        isGoldenRecord = false,

        crmId = optional("crmId"),
        operatorOhubId = optional("operatorOhubId"),
        assemblyDate = optional("assemblyDate", parseDateTimeUnsafe()),
        assetConcatId = optional("assetConcatId"),
        createdBy = optional("createdBy"),
        currency = optional("currency"),
        deliveryDate = optional("deliveryDate", parseDateTimeUnsafe()),
        lastModifiedBy = optional("lastModifiedBy"),
        name = optional("name"),
        comment = optional("comment"),
        owner = optional("owner"),
        quantityOfUnits = optional("quantityOfUnits", toInt),
        `type` = optional("type"),
        returnDate = optional("returnDate", parseDateTimeUnsafe()),
        assetStatus = optional("assetStatus"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
