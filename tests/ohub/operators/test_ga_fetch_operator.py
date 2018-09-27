# from unittest import TestCase
# from datetime import datetime
# from ohub.operators.ga_fetch_operator import GAToGSOperator, GSToLocalOperator, LocalGAToWasbOperator
# from testing.utils.test import make_dag


# class GAFetchOperatorsTestCase(TestCase):
#     def test_ga(self):
#         with make_dag('DatabricksOperatorTestCase') as dag:

#             GAToGSOperator(
#                 task_id='GAFetchOperatorsTestCase',
#             ).run(date, date, ignore_ti_state=True)

#             GSToLocalOperator(
#                 task_id='GSToLocalOperator',
#             ).run(date, date, ignore_ti_state=True)

#             LocalGAToWasbOperator(
#                 task_id='LocalGAToWasbOperator',
#             ).run(date, date, ignore_ti_state=True)
