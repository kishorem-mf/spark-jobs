package com.unilever.ohub.spark.jobs

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.{ContactPerson, Operator}
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.acm.{AcmOptions, ConcatPersonOldOhubConverter, OperatorOldOhubConverter}
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, SparkSession}

case class OldNew(oldConcatId: String, concatId: String) extends OutboundEntity


private object ContactPersonOldNewConverter extends Converter[ContactPerson, OldNew] with TypeConversionFunctions {
  override def convert(implicit cp: ContactPerson, explain: Boolean = false): OldNew =
    OldNew(
      oldConcatId = new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds).impl(cp.concatId),
      concatId = cp.concatId
    )
}

private object OperatorOldNewConverter extends Converter[Operator, OldNew] with TypeConversionFunctions {
  override def convert(implicit op: Operator, explain: Boolean = false): OldNew =
    OldNew(
      oldConcatId = new OperatorOldOhubConverter(DomainDataProvider().sourceIds).impl(op.concatId),
      concatId = op.concatId
    )
}


object ContactPersonOldNewWriter extends ExportOutboundWriter[ContactPerson] with AcmOptions {
  override def entityName(): String = "cp_old_new"

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._

    dataSet.map(ContactPersonOldNewConverter.convert(_))
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    super.run(spark, config.copy(hashesInputFile = None), storage)
  }
}

object OperatorOldNewWriter extends ExportOutboundWriter[Operator] with AcmOptions {
  override def entityName(): String = "op_old_new"

  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._

    dataSet.map(OperatorOldNewConverter.convert(_))
  }

  override def run(spark: SparkSession, config: OutboundConfig, storage: Storage): Unit = {
    super.run(spark, config.copy(hashesInputFile = None), storage)
  }
}

