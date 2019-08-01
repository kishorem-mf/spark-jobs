package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.{Chain, ContactPerson}
import com.unilever.ohub.spark.ingest.CustomParsers.{parseDateTimeUnsafe, toBoolean, toTimestamp, _}
import com.unilever.ohub.spark.ingest.{ChainEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object ChainConverter extends CommonDomainGateKeeper[Chain] with ChainEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Chain = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Chain(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = Chain.customerType,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        ohubId = Option.empty,
        isGoldenRecord = false,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        isActive = true,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        conceptName = optional("conceptName"),
        numberOfUnits = optional("numberOfUnits", toInt),
        numberOfStates = optional("numberOfStates", toInt),
        estimatedAnnualSales = optional("estimatedAnnualSales", toBigDecimal),
        estimatedPurchasePotential = optional("estimatedPurchasePotential", toBigDecimal),
        address = optional("address"),
        city = optional("city"),
        state = optional("state"),
        zipCode = optional("zipCode"),
        website = optional("website"),
        phone = optional("phone"),
        segment = optional("segment"),
        primaryMenu = optional("primaryMenu"),
        secondaryMenu = optional("secondaryMenu"),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}

