#!/usr/bin/env bash

# fpo
/usr/local/pipelines/operator_pipeline.sh
/usr/local/pipelines/contactpersons_pipeline.sh
/usr/local/pipelines/subscription_pipeline.sh

# sales
/usr/local/pipelines/product_pipeline.sh
/usr/local/pipelines/order_pipeline.sh

# activities
/usr/local/pipelines/activities_pipeline.sh
/usr/local/pipelines/questions_pipeline.sh
/usr/local/pipelines/answers_pipeline.sh
/usr/local/pipelines/loyalty_points_pipeline.sh

# campaigning
/usr/local/pipelines/campaigns_pipeline.sh
/usr/local/pipelines/campaign_sends_pipeline.sh
/usr/local/pipelines/campaign_bounces_pipeline.sh
/usr/local/pipelines/campaign_opens_pipeline.sh
/usr/local/pipelines/campaign_clicks_pipeline.sh
