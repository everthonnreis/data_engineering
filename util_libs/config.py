import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from os import getenv

load_dotenv("/home/ubuntu/.env")

class ConfigFunctions:
    #Converting the xls file using LibreOffice to access the pivot table cache
    @staticmethod
    def convert_file(file_origin, bucket_name, logger):
        logger.info('Converting xls file to ods...')
        cmd = f'libreoffice --headless --convert-to xls --outdir {bucket_name} {file_origin}'
        if os.system(cmd) == 0:
            logger.info(f'libreoffice --convert-to ods {bucket_name}{file_origin}')
        else:
            logger.error(f'Error in `{cmd}`')

    # Upload the Converted File to an Amazon S3 Bucket
    @staticmethod
    def upload_file_s3(filename: str, bucket_path: str, bucket: str, logger=None):
        client = boto3.client('s3', 
                      region_name=getenv("AWS_REGION"),
                      aws_access_key_id=getenv("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY"))
        pname = Path(filename).name
        directory_name = bucket_path + f"{pname}"
        bucket_path = directory_name 

        with open(filename, "rb") as f:
            try:
                client.upload_fileobj(f, bucket, directory_name)
                logger.info(f'Importing file to bucket: s3://{bucket}/{bucket_path}')
                os.remove(pname) 
            except Exception as e:
                if logger:
                    logger.error(f"unable to upload file {filename} {e.args}")
                else:
                    print(f"unable to upload file {filename} {e.args}")

    # Convert dataframe to parquet, remove accented columns and send to s3
    @staticmethod
    def convert_parquet(filename, sheet, file_origin, bucket, bucket_name, logger):
        df = pd.read_excel(f'{bucket_name}{file_origin}', sheet_name=sheet)
        df = df.rename(columns = {'COMBUSTÍVEL': 'COMBUSTIVEL', 'REGIÃO': 'REGIAO'}, inplace = False)
        df.to_parquet(path=filename, allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True)
        ConfigFunctions.upload_file_s3(filename, bucket_name, bucket, logger=logger)

