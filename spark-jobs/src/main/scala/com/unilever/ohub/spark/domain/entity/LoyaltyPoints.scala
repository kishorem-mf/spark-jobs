package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, LoyaltyPointsDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter, LoyaltyPointsDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object LoyaltyPointsDomainExportWriter extends DomainExportWriter[LoyaltyPoints]

object LoyaltyPoints extends DomainEntityCompanion[LoyaltyPoints] {
  override val engineFolderName: String = "loyaltypoints"
  override val auroraFolderLocation = None
  override val domainExportWriter: Option[DomainExportWriter[LoyaltyPoints]] = Some(LoyaltyPointsDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[LoyaltyPoints]] = Some(com.unilever.ohub.spark.export.acm.LoyaltyPointsOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[LoyaltyPoints]] = Some(com.unilever.ohub.spark.export.dispatch.LoyaltyPointsOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[LoyaltyPoints]] = Some(LoyaltyPointsDWWriter)
  override val auroraInboundWriter: Option[ExportOutboundWriter[LoyaltyPoints]] = Some(com.unilever.ohub.spark.datalake.LoyaltyPointsOutboundWriter)
  override val dataLakeWriter: Option[AzureDLWriter[LoyaltyPoints]] = Some(LoyaltyPointsDLWriter)
}

case class LoyaltyPoints(
                          // generic fields
                          // mandatory fields
                          id: String,
                          creationTimestamp: Timestamp,
                          concatId: String,
                          countryCode: String,
                          customerType: String,
                          sourceEntityId: String,
                          sourceName: String,
                          isActive: Boolean,
                          ohubCreated: Timestamp,
                          ohubUpdated: Timestamp,
                          // optional fields
                          dateCreated: Option[Timestamp],
                          dateUpdated: Option[Timestamp],
                          // used for grouping and marking the golden record within the group
                          ohubId: Option[String],
                          isGoldenRecord: Boolean,

                          // specific fields
                          totalEarned: Option[BigDecimal],
                          totalSpent: Option[BigDecimal],
                          totalActual: Option[BigDecimal],
                          rewardGoal: Option[BigDecimal],
                          contactPersonConcatId: Option[String],
                          contactPersonOhubId: Option[String],
                          operatorConcatId: Option[String],
                          operatorOhubId: Option[String],
                          rewardName: Option[String],
                          rewardImageUrl: Option[String],
                          rewardLandingPageUrl: Option[String],
                          rewardEanCode: Option[String],

                          // other fields
                          additionalFields: Map[String, String],
                          ingestionErrors: Map[String, IngestionError]
                        ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[LoyaltyPoints] = LoyaltyPoints
}
