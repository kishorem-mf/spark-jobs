package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export.dispatch.model._
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter}
import org.apache.spark.sql.{Dataset, SparkSession}

trait DispatcherOptions extends CsvOptions {

  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

  override val mustQuotesFields: Boolean = true
}

object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson, DispatchContactPerson] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._
    dataSet.map(ContactPersonDispatchConverter.convert(_))
  }

  override def entityName(): String = "RECIPIENTS"
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator, DispatchOperator] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorDispatchConverter.convert(_))
  }

  override def entityName(): String = "OPERATORS"
}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription, DispatchSubscription] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionDispatchConverter.convert(_))
  }

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product, DispatchProduct] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductDispatchConverter.convert(_))
  }

  override def entityName(): String = "PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order, DispatchOrder] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderDispatchConverter.convert(_))
  }

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine, DispatchOrderLine] with DispatcherOptions {
  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    dataSet.filter(o ⇒ {
      o.orderType match {
        case Some(t) ⇒ !(t.equals("SSD") || t.equals("TRANSFER"))
        case None ⇒ true
      }
    });
  }

  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[OrderLine]) = {
    import spark.implicits._
    dataSet.map(OrderLineDispatchConverter.convert(_))
  }

  override def entityName(): String = "ORDERLINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity, DispatchActivity] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityDispatcherConverter.convert(_))
  }

  override def entityName(): String = "ACTIVITIES"
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints, DispatchLoyaltyPoints] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    import spark.implicits._
    dataSet.map(LoyaltyPointsDispatcherConverter.convert(_))
  }

  override def entityName(): String = "LOYALTIES"
}

object CampaignOutboundWriter extends ExportOutboundWriter[Campaign, DispatchCampaign] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Campaign]) = {
    import spark.implicits._

    dataSet.map(CampaignDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CAMPAIGNS"
}

object CampaignBounceOutboundWriter extends ExportOutboundWriter[CampaignBounce, DispatchCampaignBounce] {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignBounce]) = {
    import spark.implicits._

    dataSet.map(CampaignBounceDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_BOUNCES"
}

object CampaignClickOutboundWriter extends ExportOutboundWriter[CampaignClick, DispatchCampaignClick] {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignClick]) = {
    import spark.implicits._

    dataSet.map(CampaignClickDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_CLICKS"
}

object CampaignOpenOutboundWriter extends ExportOutboundWriter[CampaignOpen, DispatchCampaignOpen] {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignOpen]) = {
    import spark.implicits._

    dataSet.map(CampaignOpenDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_OPENS"
}

object CampaignSendOutboundWriter extends ExportOutboundWriter[CampaignSend, DispatchCampaignSend] {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignSend]) = {
    import spark.implicits._

    dataSet.map(CampaignSendDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_CLICKS"
}



