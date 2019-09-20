#!/usr/bin/env bash

# fpo
bash /usr/local/pipelines/channel_mappings_pipeline.sh
bash /usr/local/pipelines/operator_pipeline.sh
bash /usr/local/pipelines/contactpersons_pipeline.sh
bash /usr/local/pipelines/subscription_pipeline.sh
bash /usr/local/pipelines/chains_pipeline.sh

# sales
bash /usr/local/pipelines/product_pipeline.sh
bash /usr/local/pipelines/order_pipeline.sh
bash 
# activities
bash /usr/local/pipelines/activities_pipeline.sh
bash /usr/local/pipelines/questions_pipeline.sh
bash /usr/local/pipelines/answers_pipeline.sh
bash /usr/local/pipelines/loyalty_points_pipeline.sh
 
# campaigning
bash /usr/local/pipelines/campaigns_pipeline.sh
bash /usr/local/pipelines/campaign_sends_pipeline.sh
bash /usr/local/pipelines/campaign_bounces_pipeline.sh
bash /usr/local/pipelines/campaign_opens_pipeline.sh
bash /usr/local/pipelines/campaign_clicks_pipeline.sh
