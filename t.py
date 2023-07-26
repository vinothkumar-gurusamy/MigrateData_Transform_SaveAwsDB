import csv

import requests
import zipfile
import os
import boto3
from botocore.exceptions import NoCredentialsError
import json


def download_zip_file(url, filename):
    r = requests.get(url, stream=True, headers={'User-Agent': 'Mozilla/5.0'})
    if r.status_code == 200:
        with open(filename, 'wb') as f:
            r.raw.decode_content = True
            f.write(r.content)
            print('Zip File Downloading Completed')


def extract_zip_file(currentpath, extractpath):
    if not os.path.exists(os.path.join(currentpath, extractpath)):
        os.makedirs(extractpath)
    if os.path.isdir(currentpath):
        for file in os.listdir(currentpath):
            if file.endswith('.zip'):
                with zipfile.ZipFile(os.path.join(currentpath, file)) as z:
                    z.extractall(os.path.join(currentpath + extractpath))
    else:
        print('Directory does not exist')


def load_files_into_dict(jsonfilespath):
    i = 1
    files_to_upload = {}
    for file_name in os.listdir(jsonfilespath):
        files_to_upload.update({i: file_name})
        i = i + 1
    print(files_to_upload)
    return files_to_upload


def upload_files_to_awss3(jsonfilespath, list_of_files_dict, bucket, access_key, secret_key):
    s3 = boto3.client('s3', aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
    for file_name in list_of_files_dict.values():
        try:
            local_file_path = jsonfilespath + '/' + file_name
            s3_key = file_name
            s3.upload_file(local_file_path, bucket, 'test/{}'.format(s3_key))
            print("Upload Successful")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")


def extract_data(jsonFiles, dict_list_of_files, path):
    flag = 0
    count = 0
    s = {}
    for file_name in dict_list_of_files.values():
        local_file_path = jsonFiles + '/' + file_name
        get_json_size = os.path.getsize(local_file_path)
        if count <= 2:
            if get_json_size != 2:
                try:
                    with open(local_file_path) as f:
                        jsonFileData = json.load(f)
                    entity_key = jsonFileData['cik']
                    entity_name = jsonFileData['entityName']
                    entity_dei_common_stock = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                        'label']
                    entity_dei_common_stock_desc = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                        'description']
                    entity_dei_common_stock_units = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                        'units']['shares']
                    column_name = ["cik", "entity_name", "stock_name", "stock_desc", "end_date", "value", "accn_num",
                                   "f_year", "fp_quarter",
                                   "form_num", "filed_date", "frame_details"]
                    with open(path, 'a', newline='') as fp:
                        writer = csv.DictWriter(fp, fieldnames=column_name)
                        if flag == 0:
                            writer.writeheader()
                        for i in entity_dei_common_stock_units:
                            print('hello')
                            print(i)
                            end = i.get('end')
                            val = i.get('val')
                            accn = i.get('accn')
                            fy = i.get('fy')
                            fp = i.get('fp')
                            form = i.get('form')
                            filed = i.get('filed')
                            frame = i.get('frame')
                            s.update([("cik", entity_key), ("entity_name", entity_name),
                                      ("stock_name", entity_dei_common_stock),
                                      ("stock_desc", entity_dei_common_stock_desc),
                                      ("end_date", end), ("value", val), ("accn_num", accn),
                                      ("f_year", fy), ("fp_quarter", fp),
                                      ("form_num", form), ("filed_date", filed), ("frame_details", frame)])
                            writer.writerow(s)
                    flag = flag + 1
                    count = count + 1
                except KeyError:
                    print("Key not found in json file")


def convert_csv_into_json(csv_file, json_file):
    jsonArray = []

    with open(csv_file, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            jsonArray.append(row)
    with open(json_file, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)


def upload_json_data_into_db(json_file):
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id="AKIAXQXR5MCQ7YVVUBOM",
    #                           aws_secret_access_key="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")
    # table = dynamodb.create_table(
    #     TableName='Data_Analysis',
    #     KeySchema=[
    #         {
    #             'AttributeName': 'cik',
    #             'KeyType': 'HASH'  # Partition key
    #         },
    #         {
    #             'AttributeName': 'entity_name',
    #             'KeyType': 'RANGE'  # Sort key
    #         }
    #     ],
    #     AttributeDefinitions=[
    #         {
    #             'AttributeName': 'cik',
    #             'AttributeType': 'S'
    #         },
    #         {
    #             'AttributeName': 'entity_name',
    #             'AttributeType': 'S'
    #         }
    #
    #     ],
    #     ProvisionedThroughput={
    #         'ReadCapacityUnits': 10,
    #         'WriteCapacityUnits': 10
    #     }
    # )
    #
    # print("Table status:", table.table_status)

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id="AKIAXQXR5MCQ7YVVUBOM",
                              aws_secret_access_key="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")
    table = dynamodb.Table("Data_Analysis")

    with open(json_file) as json_file:
        data = json.load(json_file)
        for i in data:
            cik = i['cik']
            entity_name = i['entity_name']
            stock_name = i["stock_name"]
            stock_desc = i["stock_desc"]
            end_date = i["end_date"]
            value = i["value"]
            accn_num = i["accn_num"]
            f_year = i["f_year"]
            fp_quarter = i["fp_quarter"]
            form_num = i["form_num"]
            filed_date = i["filed_date"]
            frame_details = i["frame_details"]

            print("Adding car:", cik, entity_name)
            table.put_item(
                Item={
                    "cik": cik,
                    "entity_name": entity_name,
                    "stock_name": stock_name,
                    "stock_desc": stock_desc,
                    "end_date": end_date,
                    "value": value,
                    "accn_num": accn_num,
                    "f_year": f_year,
                    "fp_quarter": fp_quarter,
                    "form_num": form_num,
                    "filed_date": filed_date,
                    "frame_details": frame_details
                }
            )


def main():
    myprojectpath = r'C:/Users/vinot/PycharmProjects/pythonProject/Final_Project_1/'
    extractpath = 'extractzipfiles'
    url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
    filename = url.split('/')[-1]
    # download_zip_file(url, filename)
    # # extract_zip_file(myprojectpath, extractpath)
    dict_list_of_files = load_files_into_dict(os.path.join(myprojectpath, extractpath))
    # ACCESS_KEY = input('Enter Access Key - S3 bucket - ', )
    # SECRET_KEY = input('Enter Secret Key - S3 bucket - ', )
    upload_files_to_awss3(os.path.join(myprojectpath, extractpath), dict_list_of_files, 'dataenggcoursevinoth', ACCESS_KEY,
                          SECRET_KEY)
    # extract_data(os.path.join(myprojectpath, extractpath), dict_list_of_files,
    #              os.path.join(myprojectpath, 'conslidated.csv'))
    # convert_csv_into_json(os.path.join(myprojectpath, 'conslidated.csv'),
    #                       os.path.join(myprojectpath, 'conslidated.json'))
    # upload_json_data_into_db(os.path.join(myprojectpath, 'conslidated.json'))


if __name__ == '__main__':
    main()
