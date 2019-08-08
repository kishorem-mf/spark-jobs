package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity._
import com.unilever.ohub.spark.export.{CsvOptions, ExportOutboundWriter, OutboundConfig}
import org.apache.spark.sql.{Dataset, SparkSession}

trait DispatcherOptions extends CsvOptions {

  override val delimiter: String = ";"

  override val extraOptions = Map(
    "delimiter" -> delimiter
  )

  override val mustQuotesFields: Boolean = true
}

object ContactPersonOutboundWriter extends ExportOutboundWriter[ContactPerson] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[ContactPerson]) = {
    import spark.implicits._
    dataSet.map(ContactPersonDispatchConverter.convert(_))
  }

  override def entityName(): String = "CONTACT_PERSONS"
}

object OperatorOutboundWriter extends ExportOutboundWriter[Operator] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Operator]) = {
    import spark.implicits._
    dataSet.map(OperatorDispatchConverter.convert(_))
  }

  override def entityName(): String = "OPERATORS"
}

object SubscriptionOutboundWriter extends ExportOutboundWriter[Subscription] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Subscription]) = {
    import spark.implicits._
    dataSet.map(SubscriptionDispatchConverter.convert(_))
  }

  override def entityName(): String = "SUBSCRIPTIONS"
}

object ProductOutboundWriter extends ExportOutboundWriter[Product] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Product]) = {
    import spark.implicits._
    dataSet.map(ProductDispatchConverter.convert(_))
  }

  override def entityName(): String = "ORDER_PRODUCTS"
}

object OrderOutboundWriter extends ExportOutboundWriter[Order] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Order]) = {
    import spark.implicits._
    dataSet.map(OrderDispatchConverter.convert(_))
  }

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Order], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter(!$"type".isin("SSD", "TRANSFER"));
  }

  override def entityName(): String = "ORDERS"
}

object OrderLineOutboundWriter extends ExportOutboundWriter[OrderLine] with DispatcherOptions {
  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[OrderLine], config: OutboundConfig) = {
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

  override def entityName(): String = "ORDER_LINES"
}

object ActivityOutboundWriter extends ExportOutboundWriter[Activity] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Activity]) = {
    import spark.implicits._
    dataSet.map(ActivityDispatcherConverter.convert(_))
  }

  override private[export] def filterDataSet(spark: SparkSession, dataSet: Dataset[Activity], config: OutboundConfig) = {
    import spark.implicits._
    dataSet.filter($"customerType" === "CONTACTPERSON")
  }

  override def entityName(): String = "CONTACT_PERSON_ACTIVITIES"
}

object LoyaltyPointsOutboundWriter extends ExportOutboundWriter[LoyaltyPoints] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[LoyaltyPoints]) = {
    import spark.implicits._
    dataSet.map(LoyaltyPointsDispatcherConverter.convert(_))
  }

  override def entityName(): String = "LOYALTIES"
}

object CampaignOutboundWriter extends ExportOutboundWriter[Campaign] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[Campaign]) = {
    import spark.implicits._

    dataSet.map(CampaignDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CAMPAIGNS"
}

object CampaignBounceOutboundWriter extends ExportOutboundWriter[CampaignBounce] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignBounce]) = {
    import spark.implicits._

    dataSet.map(CampaignBounceDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_BOUNCES"
}

object CampaignClickOutboundWriter extends ExportOutboundWriter[CampaignClick] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignClick]) = {
    import spark.implicits._

    dataSet.map(CampaignClickDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_CLICKS"
}

object CampaignOpenOutboundWriter extends ExportOutboundWriter[CampaignOpen] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignOpen]) = {
    import spark.implicits._

    dataSet.map(CampaignOpenDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_OPENS"
}

object CampaignSendOutboundWriter extends ExportOutboundWriter[CampaignSend] with DispatcherOptions {
  override private[export] def convertDataSet(spark: SparkSession, dataSet: Dataset[CampaignSend]) = {
    import spark.implicits._

    dataSet.map(CampaignSendDispatcherConverter.convert(_))
  }

  override def entityName(): String = "CW_SENDINGS"
}

object ChainOutboundWriter extends ExportOutboundWriter[Chain, DispatchChain] with DispatcherOptions {
  override private[spark] def convertDataSet(spark: SparkSession, dataSet: Dataset[Chain]) = {
    import spark.implicits._

    dataSet.map(ChainDispatchConverter.convert(_))
  }

  override def entityName(): String = "CHAINS"
}


