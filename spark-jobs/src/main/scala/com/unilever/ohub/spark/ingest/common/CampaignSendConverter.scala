package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignSend
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{CampaignSendEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object CampaignSendConverter extends CommonDomainGateKeeper[CampaignSend] with CampaignSendEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ CampaignSend = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      CampaignSend(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = CampaignSend.customerType,
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

        deliveryLogId = mandatory("deliveryLogId"),
        campaignId = mandatory("campaignId"),
        campaignName = optional("campaignName"),
        deliveryId = mandatory("deliveryId"),
        deliveryName = mandatory("deliveryName"),
        communicationChannel = mandatory("communicationChannel"),
        operatorConcatId = Option.empty[String],
        operatorOhubId = optional("operatorOhubId"),
        sendDate = mandatory("sendDate", parseDateTimeUnsafe()),
        isControlGroupMember = mandatory("isControlGroupMember", toBoolean),
        isProofGroupMember = mandatory("isProofGroupMember", toBoolean),
        selectionForOfflineChannels = mandatory("selectionForOfflineChannels"),
        contactPersonConcatId = Option.empty[String],
        contactPersonOhubId = mandatory("contactPersonOhubId"),
        waveName = mandatory("waveName"),

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
