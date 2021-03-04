package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter}
import com.unilever.ohub.spark.export.businessdatalake.{AzureDLWriter, WholesalerAssignmentDLWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object WholesalerAssignment extends DomainEntityCompanion[WholesalerAssignment] {
  override val engineFolderName = "wholesalerassignments"
  override val auroraFolderLocation = None
  override val domainExportWriter: Option[DomainExportWriter[WholesalerAssignment]] = None
  override val acmExportWriter: Option[ExportOutboundWriter[WholesalerAssignment]] = None
  override val dispatchExportWriter: Option[ExportOutboundWriter[WholesalerAssignment]] = None
  override val auroraInboundWriter: Option[ExportOutboundWriter[WholesalerAssignment]] = None
  override val azureDwWriter: Option[AzureDWWriter[WholesalerAssignment]] = None
  override val dataLakeWriter: Option[AzureDLWriter[WholesalerAssignment]] = Some(WholesalerAssignmentDLWriter)
}

case class WholesalerAssignment(
                     // generic fields
                     id: String,
                     creationTimestamp: Timestamp,
                     concatId: String,
                     countryCode: String,
                     customerType: String,
                     dateCreated: Option[Timestamp],
                     dateUpdated: Option[Timestamp],
                     isActive: Boolean,
                     isGoldenRecord: Boolean,
                     ohubId: Option[String],
                     operatorOhubId: Option[String],
                     operatorConcatId: Option[String],
                     sourceEntityId: String,
                     sourceName: String,
                     ohubCreated: Timestamp,
                     ohubUpdated: Timestamp,

                     // specific fields
                     isPrimaryFoodsWholesaler: Option[Boolean],
                     isPrimaryIceCreamWholesaler: Option[Boolean],
                     isPrimaryFoodsWholesalerCrm: Option[Boolean],
                     isPrimaryIceCreamWholesalerCrm: Option[Boolean],
                     wholesalerCustomerCode2: Option[String],
                     wholesalerCustomerCode3: Option[String],
                     hasPermittedToShareSsd: Option[Boolean],
                     isProvidedByCrm: Option[Boolean],
                     crmId: Option[String],

                     // other fields
                     additionalFields: Map[String, String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[WholesalerAssignment] = WholesalerAssignment
}
