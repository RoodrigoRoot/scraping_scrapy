# -*- coding: utf-8 -*-
import psycopg2
import random
import logging
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
logging.basicConfig(
    level=logging.DEBUG
)

class TutorialPipeline(object):

    def __init__(self):
        pass

    def connection_db(self):
        try:
            connection = psycopg2.connect(
            user="root",
            password="forever11",
            host="localhost",
            database="scra"
            )
            return connection
        except Exception as e:
            logging.debug("Problems with connection db", e)
            return 0

    def create_table(self, con):
        try:
            create_table_query = '''CREATE TABLE jobs
                (JOBS_TITLE           TEXT    NOT NULL,
                LOCATION         TEXT NOT NULL); '''
            cur = con.cursor()
            cur.execute(create_table_query)
            con.commit()
            logging.debug("Table created")
        except Exception as e:
            logging.debug("It's not possible create table", e)
            return 0

    def insert_values(self, con, item):
        try:
            i = 0
            cur = con.cursor()
            for query in item["job_title"]:    
                print("vuelta de for ###################################################################################")
                postgres_insert_query = """ INSERT INTO jobs ( JOBS_TITLE, LOCATION) VALUES (%s, %s)"""
                cur.execute(postgres_insert_query, (
                                                    item["job_title"][i],
                                                    item["location"][i]
                                                    ))
                i = i+1
            con.commit()
            logging.debug("Record inserted successfully into mobile table")
            return True
        except Exception as e:
            logging.debug("It's not possible insert data in the table", e)
            return 0

    def get_values_db(self, con):
        try:
            cur = con.cursor()
            query = "SELECT * FROM jobs;"
            cur.execute(query)
            mobile_records = cur.fetchall()
            for row in mobile_records:
                print("jobs_title ==", row[0])
                print("location ==", row[1],"\n")
        except Exception as e:
            logging.debug("It's not possible get data from db", e)

    def DB(self, item):
        try:
            con = self.connection_db()
            self.create_table(con)
            self.insert_values(con, item)
            print("Data from DB")
            self.get_values_db(con)
        except (Exception, psycopg2.Error) as e:
            logging.debug("Problems with DB", e)

    def process_item(self, item, spider):
        print("########################################## Ejecutando DB ################################################")
        self.DB(item)        
        print("########################################## Terminando DB ################################################")
        return item

