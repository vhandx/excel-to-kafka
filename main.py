
import os
import pandas
import logging


from dotenv import load_dotenv, find_dotenv
from json import dumps
from kafka import KafkaProducer
from pathlib import Path


load_dotenv(find_dotenv())

# pathOneIdImport = os.getenv("ONE_ID_PAY_IMPORT")
# pathOneIdArchive = os.getenv("ONE_ID_PAY_ARCHIVE")
# pathOneIdError = os.getenv("ONE_ID_PAY_ERROR")


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def process():
    load_dotenv(find_dotenv())

    myProducer = KafkaProducer(
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    load_dotenv(find_dotenv())

    pathOneId = os.getenv("ONE_ID_PAY")
    pathOneIdImport = pathOneId + os.getenv("ONE_ID_PAY_IMPORT")
    pathOneIdArchive = pathOneId + os.getenv("ONE_ID_PAY_ARCHIVE")
    pathOneIdError = pathOneId + os.getenv("ONE_ID_PAY_ERROR")

    # Doc file trong folder
    dir_list = os.listdir(pathOneIdImport)
    #
    for f in dir_list:
        if os.path.splitext(f)[1].lower() in ['.xls', '.xlsx']:
            df = pandas.read_excel('D:\\Temps\\OneId\\Import\\demo.xlsx', header=1, sheet_name='Sheet1')
            json = df.to_json()
            logging.info('----------json() - {} ----------'.format(json))
            myProducer.send("quote-requests", value=json)
            Path('D:\\Temps\\OneId\\Import\\demo.xlsx').rename('D:\\Temps\\OneId\\Archive\\demo.xlsx')



if __name__ == '__main__':
    print_hi('PyCharm')
    process()


