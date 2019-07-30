package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignOpen
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{CampaignOpenEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignOpenConverter extends CommonDomainGateKeeper[CampaignOpen] with CampaignOpenEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignOpen = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      CampaignOpen(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = CampaignOpen.customerType,
        isActive = mandatory("isActive", toBoolean),
        sourceEntityId = mandatory("sourceEntityId"),
        campaignConcatId = mandatory("campaignConcatId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        ohubId = Option.empty,
        isGoldenRecord = true, // Not specified when is true in mapping, so always golden...

        trackingId = mandatory("trackingId"),
        campaignId = mandatory("campaignId"),
        campaignName = optional("campaignName"),
        deliveryId = mandatory("deliveryId"),
        deliveryName = mandatory("deliveryName"),
        communicationChannel = mandatory("communicationChannel"),
        contactPersonConcatId = mandatory("contactPersonConcatId"),
        contactPersonOhubId = Option.empty,
        operatorConcatId = optional("operatorConcatId"),
        operatorOhubId = Option.empty,
        openDate = mandatory("openDate", parseDateTimeUnsafe()),
        deliveryLogId = optional("deliveryLogId"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
