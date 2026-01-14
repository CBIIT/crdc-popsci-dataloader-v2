#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import mgclient # type: ignore
from gqlalchemy import Memgraph # type: ignore

from icdc_schema import ICDC_Schema
from props import Props
from bento.common.utils import LOG_PREFIX, APP_NAME,  UPSERT_MODE, NEW_MODE, DELETE_MODE
from bento.common.utils import get_logger, check_schema_files
from bento.common.utils import print_config,  load_plugin #,get_log_file
import time

from Bento_config import BentoConfig
from data_loader_memgraph import DataLoader   # udpated to use new version of data loader
#from bento.common.s3_v2 import S3Bucket

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Data_Loader'
os.environ[APP_NAME] = 'Data_Loader'

#S3_PROFILE = 'default'

def parse_arguments():
    parser = argparse.ArgumentParser(description='Load TSV(TXT) files into Memgraph')
    parser.add_argument('-s', '--schema', help='Schema files', action='append')
    parser.add_argument('--prop-file', help='Property file, example is in config/props.example.yml')
    parser.add_argument('--backup-folder', help='Location to store database backup')
    parser.add_argument('config_file', help='Configuration file, example is in config/data-loader-config.example.yml',
                        nargs='?', default=None)
    parser.add_argument('-c', '--cheat-mode', help='Skip validations, aka. Cheat Mode', action='store_true')
    parser.add_argument('-dg', '--debug-mode', help='Run data Loader in debug mode', action='store_true')
    parser.add_argument('-d', '--dry-run', help='Validations only, skip loading', action='store_true')
    parser.add_argument('--wipe-db', help='Wipe out database before loading, you\'ll lose all data!',
                        action='store_true')
    parser.add_argument('--no-backup', help='Skip backup step', action='store_true')
    parser.add_argument('-y', '--yes', help='Automatically confirm deletion and database wiping', action='store_true')
    parser.add_argument('-M', '--max-violations', help='Max violations to display', nargs='?', type=int)
    parser.add_argument('-b', '--bucket', help='S3 bucket name')
    parser.add_argument('-f', '--s3-folder', help='S3 folder')
    parser.add_argument('-m', '--mode', help='Loading mode', choices=[UPSERT_MODE, NEW_MODE, DELETE_MODE],
                        default=UPSERT_MODE)
    parser.add_argument('-mb', '--manifest_bucket', help='Dataset directory')
    parser.add_argument('-mn', '--manifest_name', help='Dataset directory')
    parser.add_argument('--dataset', help='Dataset directory')
    parser.add_argument('--split-transactions', help='Creates a separate transaction for each file',
                        action='store_true')

    return parser.parse_args()


def process_arguments(args, log):
    config_file = None
    log.info("")

    if args.config_file:
        config_file = args.config_file
    else:   # if a file was not provided then use the default file
        config_file = './config/memgraph-config_v2.yml'  # used in debug mode
    config = BentoConfig(config_file)

    # Required Fields
    if args.debug_mode:
        config.debug_mode = args.debug_mode
    if args.s3_folder:
        config.s3_folder = args.s3_folder
    if args.yes:
        config.yes = args.yes

    if args.dataset:
        config.dataset = args.dataset
    if args.manifest_bucket:
        config.manifest_bucket = args.manifest_bucket
    if args.manifest_name:
        config.manifest_name = args.manifest_name

    if not config.dataset and not config.s3_folder:
        log.error('No dataset specified! Please specify a dataset in config file or with CLI argument --dataset')
        sys.exit(1)
    if not config.s3_folder and not os.path.isdir(config.dataset):
        log.error('{} is not a directory!'.format(config.dataset))
        sys.exit(1)

    if args.prop_file:
        config.prop_file = args.prop_file
    if not config.prop_file:
        log.error('No properties file specified! ' +
                  'Please specify a properties file in config file or with CLI argument --prop-file')
        sys.exit(1)

    if args.schema:
        config.schema_files = args.schema
    if not config.schema_files:
        log.error('No schema file specified! ' +
                  'Please specify at least one schema file in config file or with CLI argument --schema')
        sys.exit(1)


    # Conditionally Required Fields
    if args.split_transactions:
        config.split_transactions = args.split_transactions
    if args.no_backup:
        config.no_backup = args.no_backup
    if args.backup_folder:
        config.backup_folder = args.backup_folder
    if config.split_transactions and config.no_backup:
        log.error('--split-transaction and --no-backup cannot both be enabled, a backup is required when running'
                  ' in split transactions mode')
        sys.exit(1)
    if not config.backup_folder and not config.no_backup:
        log.error('Backup folder not specified! A backup folder is required unless the --no-backup argument is used')
        sys.exit(1)

    if not config.convert_files:
        config.convert_files = []

    file_list = []
#    if config.s3_folder:
#        if args.bucket:
#            config.s3_bucket = args.bucket
#        if not config.s3_bucket:
#            log.error('Please specify S3 bucket name with -b/--bucket argument!')
#            sys.exit(1)
        #bucket = S3Bucket(config.s3_bucket, S3_PROFILE)
        #log.info(f'Getting data from s3://{config.s3_bucket}/{config.s3_folder}')

        #response = bucket.client.list_objects_v2(Bucket=config.s3_bucket, Prefix=config.s3_folder)
        #if response["KeyCount"] > 0:
        #    file_list = file_list + [f"s3://{config.s3_bucket}/" + i["Key"] for i in response["Contents"] if i["Key"][-3:] != "log"]

            # df = pd.read_csv(s3_file_path)
#        else:
#            log.info('No Files were found in this directory')
#            file_list = []


    # Optional Fields

    if args.wipe_db:
        config.wipe_db = args.wipe_db

    if args.yes:
        config.yes = args.yes

    if args.dry_run:
        config.dry_run = args.dry_run

    if args.cheat_mode:
        config.cheat_mode = args.cheat_mode

    if args.mode:
        config.loading_mode = args.mode
    if not config.loading_mode:
        config.loading_mode = "UPSERT_MODE"

    if args.max_violations:
        if type(args.max_violations) not in [int, float]:
            log.warning("max vilations is not a number, defaultin to 10")
            args.max_violations = 10
        elif args.max_violations < 0:
            log.warning("max vilations is negative which is not allowed, defaultin to 10")
            args.max_violations = 10
        config.max_violations = int(args.max_violations)
    if not config.max_violations:
        config.max_violations = 10
        
        
#    false_logic_list = ["dry_run", "wipe_db"]  #default to false
#    true_logic_list = ["cheat_mode",  "no_backup", "no_confirmation", "split_transactions"] #default to true
    
#    for curr_item in true_logic_list:
#        curr_case = eval(f"config.{curr_item}")
#        if type(curr_case) != bool:
#            log.warning(f"{curr_item} was not supplied as a logical (received: {curr_case}), defaulting to False")
#            setattr(config, curr_item, True)

#    for curr_item in false_logic_list:
#        curr_case = eval(f"config.{curr_item}")
#        if type(curr_case) != bool:
#            log.warning(f"{curr_item} was not supplied as a logical (received: {curr_case}), defaulting to False")
#            setattr(config, curr_item, False)

    
    return config, file_list


#def upload_log_file(bucket_name, folder, file_path):
#    base_name = os.path.basename(file_path)
#    s3 = S3Bucket(bucket_name, S3_PROFILE)
#    key = f'{folder}/{base_name}'
#    return s3.upload_file(key, file_path)


def prepare_plugin(config, schema):
    if not config.params:
        config.params = {}
    config.params['schema'] = schema
    return load_plugin(config.module_name, config.class_name, config.params)


def show_log_handlers(root_logger):
    """
    Prints the handlers associated with the root logger and all other named loggers.
    """
    print(f"Root Logger: {root_logger}")
    if not root_logger.handlers:
        print("  No handlers configured for the root logger.")
    for handler in root_logger.handlers:
        print(f"  - {handler}")


# Data loader will try to load all TSV(.TXT) files from given directory into Neo4j
# optional arguments includes:
# -i or --uri followed by Neo4j server address and port in format like bolt://12.34.56.78:7687
def main():
    log = get_logger('Loader')
#    log_file = get_log_file()
    overall_start = time.perf_counter()

    config, file_list = process_arguments(parse_arguments(), log)
    print_config(log, config)

    if not check_schema_files(config.schema_files, log):
        return

    try:
        log.info("")
#        if config.s3_bucket and len(file_list) > 0:
#            log.info("file list will be loaded using the s3 bucket provided in manifest")

        if config.dataset:
            txt_files = glob.glob('{}/*.txt'.format(config.dataset))
            tsv_files = glob.glob('{}/*.tsv'.format(config.dataset))
            csv_files = glob.glob('{}/*.csv'.format(config.dataset))
            txt_files_lvl2 = glob.glob('{}/*/*.txt'.format(config.dataset))
            tsv_files_lvl2 = glob.glob('{}/*/*.tsv'.format(config.dataset))
            csv_files_lvl2 = glob.glob('{}/*/*.csv'.format(config.dataset))
            file_list = file_list + txt_files + tsv_files + csv_files + txt_files_lvl2 + tsv_files_lvl2  + csv_files_lvl2
        else:
            log.error('Local Mode was specified and No dataset was provided! ')
            log.error('Please specify a dataset in config file or with CLI argument --dataset')
            sys.exit(1)

        if file_list:
            if config.wipe_db and not config.yes:
                if not confirm_deletion('Wipe out entire Neo4j database before loading?'):
                    sys.exit(1)

            if config.loading_mode == DELETE_MODE and not config.yes:
                if not confirm_deletion('Delete all nodes and child nodes from data file?'):
                    sys.exit(1)

            if config.dataset:
                prop_path = os.path.join(config.dataset, config.prop_file)
            else:
                prop_path = config.prop_file
            if os.path.isfile(prop_path):
                props = Props(prop_path)
            else:
                props = Props(config.prop_file)
            schema = ICDC_Schema(config.schema_files, props)
            conn = mgclient.connect(host='127.0.0.1', port=7687)
            memgraph = Memgraph() # Or connect to your Memgraph instance

            plugins = []
            if len(config.plugins) > 0:
                for plugin_config in config.plugins:
                    plugins.append(prepare_plugin(plugin_config, schema))
            loader = DataLoader(conn, memgraph, schema, config.database_name, config.convert_files, plugins)
            
            load_result = False
            if len(file_list) > 0:  # only call loader if files were found
                load_result = loader.load(file_list, config.cheat_mode, config.dry_run, config.loading_mode,
                                          config.wipe_db, config.max_violations, split=config.split_transactions,
                                          no_backup=config.no_backup,
                                          backup_folder=config.backup_folder)
                #create_summary_data(driver)
            if load_result is False:
                log.error('Data files upload failed')
                sys.exit(1)
        else:
            log.info('No files to load.')
    except mgclient.OperationalError:
        log.critical("couldn't connect to host: Connection refused.")

    except Exception as e:
        log.critical(e)
        return
    except KeyboardInterrupt:
        log.critical("User stopped the loading!")
        return
    finally:
        if conn: # type: ignore
            conn.close()
        # added print statments to show total duration program took tor
        log.info("")
        log.info("driver has been closed")
        log.info("")
        if len(file_list) > 0:  # only print summary stats if loader was called
            log.info("Total time from start of load function to completion  " +
                     f"{time.perf_counter() - overall_start:.4f} seconds")
            log.info("")
            log.info("Summary: ")
            log.info(f"Time to wipe old database: {loader.wipe_timer:.2f} seconds")
            log.info(f"Time to create dictionary for existing nodes: {loader.create_dict_timer:.2f} seconds")
            log.info(f"Nodes Created / Updated: {loader.load_passed:,d} in {loader.load_node_time:.2f} seconds")
            log.info(f"Time to update dictionary for new nodes: {loader.update_dict_timer:.2f} seconds")
            log.info(f"Relationships Created / Updated: {loader.relationship_passed:,d} in " +
                     f"{loader.load_relation_time:.2f} seconds")

#    if config.s3_bucket and config.s3_folder:
#        result = upload_log_file(config.s3_bucket, f'{config.s3_folder}/logs', log_file)
#        if result:
#            log.info(f'Uploading log file {log_file} succeeded!')
#        else:
#            log.error(f'Uploading log file {log_file} failed!')


def confirm_deletion(message):
    print(message)
    confirm = input('Type "yes" and press enter to proceed (You\'ll LOSE DATA!!!), press enter to cancel:')
    confirm = confirm.strip().lower()
    return confirm == 'yes'


if __name__ == '__main__':
    main()
