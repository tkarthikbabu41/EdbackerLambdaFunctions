from datetime import datetime
import json
import pymysql.cursors
import os
import time, math, random
import hashlib
import string
import re


def create_notificationservice(params):
        print("Admin Notification creation")
        print("Connect DB creation started")
        print(pymysql.cursors)

        eb_connection = pymysql.connect(host=os.environ['eb_endpoint'], user=os.environ['eb_username'], passwd=os.environ['eb_password'], db=os.environ['eb_database_name'])

        print("DB connection")
        print(eb_connection)

        cursor = eb_connection.cursor()
        notificationserviceCreateSql = "INSERT INTO `edb_push_notification_service` (`subject`, `messageBody`, `sent_user_id`, `recevier_user_id`, `recevier_user_type`, `isRead`, `notificationType`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (params['subject'], params['messageBody'], params['sent_user_id'], params['recevier_user_id'], params['recevier_user_type'], 0, params['notificationType'])

        print(notificationserviceCreateSql)
        cursor.execute(notificationserviceCreateSql, val)
        eb_connection.commit()
        print("Updated edb_users table")


def lambda_handler(event, context):
    for record in event['Records']:
        payload=record["body"]
        print(payload)
        payload=json.loads(str(payload))
        print("Request Params:")
        print(payload)
        create_notificationservice(payload);

        print("--------------------END--------------------")