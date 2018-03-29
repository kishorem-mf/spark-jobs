#!/usr/bin/python
import datetime
import time

import oauth2client.client
from googleapiclient import discovery
from config import country_codes


def main(**kwargs):

    ds = kwargs['ds']

    from_date = datetime.datetime.strptime(ds, '%Y-%m-%d') - datetime.timedelta(days=1)
    to_date = datetime.datetime.strptime(ds, '%Y-%m-%d')
    dates = [from_date + datetime.timedelta(days=x) for x in range(0, (to_date - from_date).days + 1)]

    print "Exporting BigQuery to AVRO"
    print "Dates: {}".format(map(lambda d: d.strftime("%Y-%m-%d"), dates))

    project_id_gs = "ufs-accept"
    project_id_bq = "digitaldataufs"
    running_jobs = []

    # Expects GOOGLE_APPLICATION_CREDENTIALS to point to a .json file containing credentials
    bq_oauth_credentials = oauth2client.client.GoogleCredentials.get_application_default()
    bq_client = discovery.build("bigquery", "v2", credentials=bq_oauth_credentials)

    for country in country_codes:
        for date_to_process in dates:
            bq_date_string = date_to_process.strftime("%Y%m%d")
            avro_date_string = date_to_process.strftime("%Y-%m-%d")

            load_config = {
                "configuration": {
                    "extract": {
                        "destinationFormat": "AVRO",
                        "sourceTable": {
                            "projectId": project_id_bq,
                            "tableId": "ga_sessions_" + bq_date_string,
                            "datasetId": country_codes[country]
                        },
                        "destinationUri": [
                            "gs://" + project_id_gs + "/ga_data/PARTITION_DATE=" + avro_date_string
                            + "/COUNTRY_CODE=" + country + "/ga_data.avro"]
                    }
                }
            }

            print "Exporting " + country + " for " + avro_date_string
            result = bq_client.jobs().insert(projectId=project_id_bq, body=load_config).execute()

            if result["status"]["state"] in ["RUNNING", "PENDING"]:
                running_jobs.append(result["jobReference"]["jobId"])
            else:
                print "Error starting job: {}".format(result)
                exit(1)

    all_jobs_succeeded = True

    while len(running_jobs) > 0:
        time.sleep(2)
        print "Polling {} job(s)".format(len(running_jobs))
        for job_id in running_jobs:
            result = bq_client.jobs().get(projectId=project_id_bq, jobId=job_id).execute()
            if result["status"]["state"] == "DONE":
                if "errors" in result["status"]:
                    print "ERROR in job :{}".format(result)
                    all_jobs_succeeded = False
                print "Job {} done. Final status: {}".format(job_id, result["status"])
                running_jobs.remove(job_id)

    if all_jobs_succeeded:
        print "Done"
    else:
        print "Not all jobs completed successfully"
        exit(1)
