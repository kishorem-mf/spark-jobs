package com.unilever.ohub.spark.jobs

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator}
import com.unilever.ohub.spark.export.acm.{AcmOptions, ConcatPersonOldOhubConverter, OperatorOldOhubConverter}
import com.unilever.ohub.spark.export.{Converter, ExportOutboundWriter, OutboundEntity, TransformationFunctions}
import org.apache.spark.sql.{Dataset, SparkSession}

case class OldNew(oldConcatId: String, concatId: String) extends OutboundEntity


private object ContactPersonOldNewConverter extends Converter[ContactPerson, OldNew] with TransformationFunctions {
  override def convert(cp: ContactPerson): OldNew =
    OldNew(
      oldConcatId = new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds).convert(cp.concatId),
      concatId = cp.concatId
    )
}

private object OperatorOldNewConverter extends Converter[Operator, OldNew] with TransformationFunctions {
  override def convert(op: Operator): OldNew =
    OldNew(
      oldConcatId = new OperatorOldOhubConverter(DomainDataProvider().sourceIds).convert(op.concatId),
      concatId = op.concatId
    )
}


object ContactPersonOldNewWriter extends ExportOutboundWriter[ContactPerson, OldNew] with AcmOptions {
  override def entityName(): String = "cp_old_new"

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._

    dataSet.map(ContactPersonOldNewConverter.convert(_))
  }
}

object OperatorOldNewWriter extends ExportOutboundWriter[Operator, OldNew] with AcmOptions {
  override def entityName(): String = "op_old_new"

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._

    dataSet.map(OperatorOldNewConverter.convert(_))
  }
}

