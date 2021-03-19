#!/bin/python3
import os
import sys
import logging
import pandas as pd
from database import Database
logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)

class EtlScript:
    def __init__(self):
        self.database_conn = Database("acme")
        self.header_file = "headers.txt"
        self.data_file = "data.csv"
        self.out_file = "output.csv"

    def load_file_to_database(self, file_path: str):
        #NOTE: I assumed that this is not asked to implement so not implementing
        #for loading data into a table I will use sql COPY command with "|" delimiter, 
        self.database_conn.load_file(file_path)

    def generate_output_csv_file(self, header_df, csv_to_data_df):
        if(header_df.shape[1] != csv_to_data_df.shape[1]):
            validation_fail = "Columns didn't match between '{}' count:'{}' and '{}' count:{}"
            validation_fail = validation_fail.format(self.header_file, header_df.shape[0], self.data_file, csv_to_data_df.shape[0])
            raise ValueError(validation_fail)
        header_data_df = header_df.append(csv_to_data_df)
        logger.info("generate_output_csv_file header and data merged df:\n %s", header_data_df.head())
        header_data_df.to_csv(self.out_file, index=False, sep= "|", encoding='utf-8', header=None)
        return header_data_df.shape[0]

    def get_file_to_df(self, input_file, file_delimiter):
        logger.info("get_file_to_df input_file:%s, file_delimiter:%s", input_file, file_delimiter)
        parsed_file_df = pd.read_csv(input_file, header=None, dtype=str, index_col=False, delimiter=file_delimiter)
        if len(parsed_file_df) != 0:
            logger.info("get_file_to_df parsed_file_df shape:%s,\n DF:\n %s", parsed_file_df.shape, parsed_file_df.head())
        return parsed_file_df

    def merge_input_files_and_generate_csv(self):
        file_delimiter = "|"
        logger.info("Merging input .txt and .csv files and generating .csv file...")
        txt_header_df = self.get_file_to_df(self.header_file, file_delimiter)
        header_df = pd.DataFrame(txt_header_df.values.tolist()).transpose()
        csv_to_data_df = self.get_file_to_df(self.data_file, file_delimiter)
        record_count = self.generate_output_csv_file(header_df, csv_to_data_df)
        return record_count

    def run(self):
        try:
            record_count = self.merge_input_files_and_generate_csv()
            self.load_file_to_database(self.out_file)
            logger.info("MarletteFunding ETL File(s) processed and loaded to DB successfully, total records(including header):%s", record_count)
        except Exception as e:
            logger.exception("Exception while processing MarletteFunding input ETL files, no data loaded to DB Reason: %s", e)

if __name__ == "__main__":
    EtlScript().run()