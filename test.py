import csv

import requests
import zipfile
import os
import boto3
from botocore.exceptions import NoCredentialsError
import json


def download_zip_files(url, filename):
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
    print("inside")
    for file_name in dict_list_of_files.values():
        local_file_path = jsonFiles + '/' + file_name
        get_json_size = os.path.getsize(local_file_path)

        if get_json_size != 2:
            try:
                with open(local_file_path) as f:
                    jsonFileData = json.load(f)
                # print(jsonFileData)
                entity_key = jsonFileData['cik']
                entity_name = jsonFileData['entityName']
                entity_dei_common_stock = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'label']
                entity_dei_common_stock_desc = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'description']
                entity_dei_common_stock_units = jsonFileData['facts']['dei']['EntityCommonStockSharesOutstanding'][
                    'units']['shares']
                entity_dei_common_publicfloat_units = jsonFileData['facts']['dei']['EntityPublicFloat'][
                    'units']['USD']
                # print(entity_dei_common_stock_units)
                # print("hello")
                # entity_dei_public_float = jsonFileData['facts']['dei']['EntityPublicFloat'][
                #     'label']
                # entity_dei_public_float_desc = jsonFileData['facts']['dei']['EntityPublicFloat'][
                #     'description']
                # entity_common_payment_name = jsonFileData['facts']['dei']['PaymentOfFinancingAndStockIssuanceCosts']['label']
                print(entity_key)
                print(entity_name)
                # print(entity_dei_common_stock)
                # print(entity_dei_common_stock_desc)
                # print(entity_dei_public_float)
                # print(entity_dei_public_float_desc)
                # l1 = [(entity_key, entity_name, entity_dei_common_stock, entity_dei_common_stock_desc,
                #        entity_dei_public_float, entity_dei_public_float_desc)]
                # l2 = [l1]
                s = []
                column_name = ["cik", "entity_name", "stock_name", "stock_desc", "end", "val", "accn", "fy", "fp",
                               "form", "filed", "frame"]
                with open(path, 'a', newline='') as fp:
                    writer = csv.writer(fp)  # this is the writer object
                    writer.writerow(column_name)
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
                        s.append((entity_key, entity_name, entity_dei_common_stock, entity_dei_common_stock_desc, end,
                                  val, accn, fy, fp, form, filed, frame))
                    writer.writerows(s)
                    break
            except KeyError:
                print("Key not found in json file")
    # print(l2)


def main():
    myprojectpath = r'C:/Users/vinot/PycharmProjects/pythonProject/Final_Project_1/'
    extractpath = 'extractzipfiles'
    url = 'https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip'
    filename = url.split('/')[-1]
    # download_zip_files(url, filename)
    # extract_zip_file(myprojectpath, extractpath)
    dict_list_of_files = load_files_into_dict(os.path.join(myprojectpath, extractpath))
    # ACCESS_KEY = input('Enter Access Key - S3 bucket - ', )
    # SECRET_KEY = input('Enter Secret Key - S3 bucket - ', )
    # upload_files_to_awss3(os.path.join(myprojectpath, extractpath), dict_list_of_files, 'dataenggcoursevinoth', ACCESS_KEY,
    #                       SECRET_KEY)
    extract_data(os.path.join(myprojectpath, extractpath), dict_list_of_files,
                 os.path.join(myprojectpath, 'conslidated.csv'))


if __name__ == '__main__':
    main()
