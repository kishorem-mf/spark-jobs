from datetime import datetime, timedelta

email_addresses = ['timvancann@godatadriven.com',
                   'Dennis.Vis@unilever.com',
                   'constantijnvisinescu@binx.io',
                   'Tycho.Grouwstra@unilever.com',
                   'Roderik-von.Maltzahn@unilever.com',
                   'Gerard.Glind@unilever.com']

country_codes = dict(
    AU=149299102,
    NZ=149386192,
    BE=136496201,
    FR=136417566,
    NL=136443158,
    # CN=,
    AT=136478148,
    DE=136487004,
    CH=136472077,
    # IN=,
    IL=149664234,
    GR=149621988,
    IT=149555300,
    MQ=155123886,
    LK=159477256,
    PK=159465213,
    SA=149449826,
    HK=149656154,
    TW=149647289,
    # KR=,
    CA=136493502,
    US=136408293,
    CZ=149431770,
    SK=155336641,
    EE=163567408,
    PL=149439115,
    CO=149633268,
    MX=149602702,
    # LA=,
    # DK=,
    FI=161738564,
    NO=161745261,
    # SE=,
    PT=149305761,
    RU=149644884,
    ZA=136119346,
    ID=142974636,
    MY=149419183,
    PH=149403978,
    SG=149358335,
    TH=149424309,
    VN=152930457,
    BG=159483761,
    HU=155330595,
    RO=155294811,
    AR=162357462,
    BR=142986451,
    CL=161669630,
    ES=136477925,
    TR=149299194,
    IE=162648003,
    GB=136489308)


def on_failure_callback(context):
    from airflow.operators.slack_operator import SlackAPIPostOperator
    from airflow.models import Variable

    template = """
:skull: Spark task *{task_id}* in *{dag_id}* failed at _{time}_
> airflow log: {airflow_log}
> databricks log: {databricks_log}
    """

    slack_token = Variable.get('slack_airflow_token')
    operator = SlackAPIPostOperator(
        task_id='slack_failure_notification',
        token=slack_token,
        channel='#airflow',
        text=template.format(task_id=context['task'].task_id,
                             dag_id=context['dag'].dag_id,
                             time=context['ts'],
                             airflow_log=context['task_instance'].log_filepath,
                             databricks_log=context['task_instance'].output_encoding),

        owner='Failure Handler')
    return operator.execute(context=context)
