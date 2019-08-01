package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{CampaignEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignConverter extends CommonDomainGateKeeper[Campaign] with CampaignEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Campaign = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Campaign(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = Campaign.customerType,
        isActive = mandatory("isActive", toBoolean),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        campaignConcatId = mandatory("campaignConcatId"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        ohubId = Option.empty,
        isGoldenRecord = true, // Not specified when is true in mapping, so always golden...

        contactPersonConcatId = mandatory("contactPersonConcatId"),
        contactPersonOhubId = Option.empty,
        campaignId = mandatory("campaignId"),
        campaignName = optional("campaignName"),
        deliveryId = mandatory("deliveryId"),
        deliveryName = mandatory("deliveryName"),
        campaignSpecification = optional("campaignSpecification"),
        campaignWaveStartDate = mandatory("campaignWaveStartDate", parseDateTimeUnsafe()),
        campaignWaveEndDate = mandatory("campaignWaveEndDate", parseDateTimeUnsafe()),
        campaignWaveStatus = mandatory("campaignWaveStatus"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
