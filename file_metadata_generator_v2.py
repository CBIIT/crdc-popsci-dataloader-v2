from prefect import flow, task
import boto3
import hashlib
import csv
import io
import os
from botocore.exceptions import ClientError
from bento.common.utils import get_uuid, get_logger, get_log_file
import toml
from datetime import datetime


prefect_home = os.path.expanduser("~/.prefect")
config_file_path = os.path.join(prefect_home, "config.toml")


@task
def get_config_file():
    try:
        with open(config_file_path, 'r') as f:
            config_data = toml.load(f)
        print("Prefect configuration loaded successfully:")
        return config_data["config_settings"]
    except FileNotFoundError:
        print(f"Error: The file '{config_file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred while loading the TOML file: {e}")
    return {}


@task
def get_boto_session(profile_name: str):
    return boto3.Session(profile_name=profile_name)


@task
def get_org_md5(s3_client, bucket, obj_key):
    response = s3_client.get_object(Bucket=bucket, Key=obj_key)
    object_content = response['Body'].read()
    md5_hash = hashlib.md5()
    md5_hash.update(object_content)
    return md5_hash.hexdigest()


@task
def check_manifest(config_data, s3_client):
    if "Transfer_Manifest_Name" not in config_data:
        print("Manifest was not provided in the config file, cannot continue")
        return []
    else:
        if config_data['Input_Folder'] == '':  # if only one level of folders was used
            obj_key = f"{config_data['Study_Folder']}/{config_data['Transfer_Manifest_Name']}"
        else:  # if the study folder is under a main input folder
            obj_key = f"{config_data['Input_Folder']}/{config_data['Study_Folder']}/{config_data['Transfer_Manifest_Name']}"
        try:
            response = s3_client.get_object(Bucket=config_data["Input_Bucket"], Key=obj_key)
            tsv_content = response['Body'].read().decode('utf-8')
            csv_file = io.StringIO(tsv_content)
            log.info(f"{obj_key} was found in {config_data['Input_Bucket']}")
            return csv_file
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                log.error(f"{obj_key} does not exist in bucket: {config_data['Input_Bucket']}")
            else:
                raise
            return []


@task
def read_manifest(csv_file):
    files = []
    csv_reader = csv.DictReader(csv_file, delimiter='\t')
    for info in csv_reader:
        files.append(info)
    return files


@task
def fetch_file_metadata(s3_client, config_data, manifest_info):
    metadata_list = []
    index_list = []

    files_in_manifest = [i["data_file_name"] for i in manifest_info]
    if config_data['Input_Folder'] == "":
        file_key = f"{config_data['Study_Folder']}"
    else:
        file_key = f"{config_data['Input_Folder']}/{config_data['Study_Folder']}"
    response = s3_client.list_objects_v2(Bucket=config_data["Input_Bucket"], Prefix=file_key)

    files_in_bucket = [i["Key"] for i in response.get("Contents", [])]
    files_in_bucket_names = [os.path.split(i)[1] for i in files_in_bucket]
    # files_in_bucket_names = [i[:-4] for i in files_in_bucket_names]  # removes extension

    for file in files_in_manifest:
        if file not in files_in_bucket_names:
            print(f"{file}: not found in {file_key}")
            continue

        curr_file = [i for i in manifest_info if i["data_file_name"] == file][0]

        file_parts = file.split('/')
        file_name_only = file_parts[-1]
        file_location = f"s3://{config_data['Input_Bucket']}/{file_key}/{file}"

        curr_file = {k.strip(): v for k, v in curr_file.items()}

        try:
            head_response = s3_client.head_object(Bucket=config_data["Input_Bucket"], Key=f"{file_key}/{file}")
            file_size = head_response['ContentLength']
            md5_calc = get_org_md5.fn(s3_client, config_data["Input_Bucket"], f"{file_key}/{file}")
            file_uuid = get_uuid(config_data["Domain"], "file", file_location)
        except Exception as e:
            print(e)

        file_dict_info = {
            'type': curr_file["type"],
            'study.study_short_name': curr_file["study.study_short_name"],
            'data_file_name': curr_file["data_file_name"],  # file_name_only,
            'data_file_type': curr_file["data_file_type"],
            'data_file_description': curr_file['data_file_description'],
            'uuid': file_uuid,
            'file_size': file_size,
            'md5sum': md5_calc,
            'file_status': 'uploaded',
            'file_location': file_location,
            'file_format': file_name_only.split('.')[-1],
            'data_file_access_control': curr_file["data_file_access_control"]
        }

        index_dict_info = {
            "guid": f"{config_data['Index_GUID_Prefix']}{file_uuid}",
            'md5': md5_calc,
            'size': file_size,
            'acl': "['*']",
            'authz': "['/open']",
            'urls': file_location
        }

        metadata_list.append(file_dict_info)
        index_list.append(index_dict_info)

    return metadata_list, index_list


@task
def write_csv(data_list, s3_client, config_file, file_name):

    response = s3_client.list_objects_v2(Bucket=config_file["Output_Bucket"], Prefix=config_file["Output_Folder"])
    md5_list = []
    if "Contents" in response:  # check to make sure files are in S3
        for curr_file in [i["Key"] for i in response["Contents"]]:
            md5_list.append(get_object_md5(s3_client, config_file["Output_Bucket"], curr_file))

    if data_list:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extension = 'tsv'
        output_path = f"{config_file['Output_Folder']}/{config_file['Study_Folder']}/{file_name}_{timestamp}.{extension}"
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, delimiter='\t', fieldnames=list(data_list[0].keys()))
        writer.writeheader()
        writer.writerows(data_list)
        csv_string = csv_buffer.getvalue()
        csv_bytes = csv_string.encode('utf-8')
        md5_hash = hashlib.md5(csv_bytes).hexdigest()
        if md5_hash in md5_list:
            print("File already exists in AWS")
        else:
            print("Writting new output file")
            s3_client.put_object(Bucket=config_file["Output_Bucket"], Key=output_path, Body=csv_buffer.getvalue())


@task
def get_object_md5(s3_client, bucket, key):
    try:
        obj_data = s3_client.get_object(Bucket=bucket, Key=key)['Body'].read()
        return hashlib.md5(obj_data).hexdigest()
    except Exception as e:
        print(f"Error computing MD5 for {key}: {e}")
        return None


@flow
def s3_metadata_to_csv(profile_name: str = "Popsci_Dev"):
    config_data = get_config_file()
    session = get_boto_session(profile_name)
    s3_client = session.client('s3')

    csv_reader = check_manifest(config_data, s3_client)
    if isinstance(csv_reader, list):
        return  # Error already logged

    manifest_info = read_manifest(csv_reader)
    metadata, index_file = fetch_file_metadata(s3_client, config_data, manifest_info)

    if not metadata:
        print("No matching files found for supplied manifest. Unable to make output files.")
        return

    write_csv(metadata, s3_client, config_data, "Neo4j_Output")
    write_csv(index_file, s3_client, config_data, "Index_Output")


if __name__ == "__main__":
    log = get_logger('Loader')
    log_file = get_log_file()
    s3_metadata_to_csv()
