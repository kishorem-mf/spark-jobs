package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignClick
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{CampaignClickEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignClickConverter extends CommonDomainGateKeeper[CampaignClick] with CampaignClickEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignClick = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      CampaignClick(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = CampaignClick.customerType,
        isActive = mandatory("isActive", toBoolean),
        sourceEntityId = mandatory("sourceEntityId"),
        campaignConcatId = mandatory("campaignConcatId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        ohubId = Option.empty[String],
        isGoldenRecord = true, // Not specified when is true in mapping, so always golden...

        trackingId = mandatory("trackingId"),
        clickedUrl = mandatory("clickedUrl"),
        clickDate = mandatory("clickDate", parseDateTimeUnsafe()),
        communicationChannel = mandatory("communicationChannel"),
        campaignId = mandatory("campaignId"),
        campaignName = optional("campaignName"),
        deliveryId = mandatory("deliveryId"),
        deliveryName = mandatory("deliveryName"),
        contactPersonConcatId = Option.empty[String],
        contactPersonOhubId = mandatory("contactPersonOhubId"),
        isOnMobileDevice = mandatory("isOnMobileDevice", toBoolean),
        operatingSystem = optional("operatingSystem"),
        browserName = optional("browserName"),
        browserVersion = optional("browserVersion"),
        operatorConcatId = Option.empty[String],
        operatorOhubId = optional("operatorOhubId"),
        deliveryLogId = optional("deliveryLogId"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
