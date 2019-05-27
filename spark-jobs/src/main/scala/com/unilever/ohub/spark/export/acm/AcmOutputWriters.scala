package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export.acm.model._
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter}
import org.apache.spark.sql.{Dataset, SparkSession}

trait AcmOptions extends CsvOptions {

  override val delimiter: String = "\u00B6"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

}

object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson, AcmContactPerson] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._
    dataSet.map(ContactPersonAcmConverter.convert(_))
  }

  override def entityName(): String = "RECIPIENTS"
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator, AcmOperator] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorAcmConverter.convert(_))
  }

  override def entityName(): String = "OPERATORS"
}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription, AcmSubscription] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionAcmConverter.convert(_))
  }

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product, AcmProduct] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductAcmConverter.convert(_))
  }

  override def entityName(): String = "PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order, AcmOrder] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderAcmConverter.convert(_))
  }

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine, AcmOrderLine] with AcmOptions {
  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    dataSet.filter(o ⇒ {
      o.orderType match {
        case Some(t) ⇒ !(t.equals("SSD") || t.equals("TRANSFER"))
        case None    ⇒ true
      }
    });
  }

  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderLineAcmConverter.convert(_))
  }

  override def entityName(): String = "ORDERLINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity, AcmActivity] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityAcmConverter.convert(_))
  }

  override def entityName(): String = "ACTIVITIES"
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints, AcmLoyaltyPoints] with AcmOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    import spark.implicits._
    dataSet.map(LoyaltyPointsAcmConverter.convert(_))
  }

  override def entityName(): String = "LOYALTIES"
}

