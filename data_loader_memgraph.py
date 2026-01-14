#!/usr/bin/env python3

import os
from collections import deque
import csv
import re
import datetime
import sys
import platform
import subprocess
import json
import time
from timeit import default_timer as timer
from bento.common.utils import get_host, DATETIME_FORMAT, reformat_date
# import copy

from neo4j import Driver
import pandas as pd
import numpy as np
from datetime import date


#from cancer_translation_func import get_cancer_translations, get_cancer_translations_list, convert_codes

from icdc_schema import ICDC_Schema, is_parent_pointer, get_list_values
from bento.common.utils import get_logger, NODES_CREATED, RELATIONSHIP_CREATED, UUID, \
    RELATIONSHIP_TYPE, MULTIPLIER, ONE_TO_ONE, UPSERT_MODE, \
    NEW_MODE, DELETE_MODE, NODES_DELETED, RELATIONSHIP_DELETED, combined_dict_counters, \
    MISSING_PARENT, get_string_md5  # DEFAULT_MULTIPLIER,  NODE_LOADED   #these libraries are not used


NODE_TYPE = 'type'
PROP_TYPE = 'Type'
PROPERTIES = 'Props'
PARENT_TYPE = 'parent_type'
PARENT_ID_FIELD = 'parent_id_field'
PARENT_ID = 'parent_id'
excluded_fields = {NODE_TYPE}
CASE_NODE = 'case'
CASE_ID = 'case_id'
CREATED = 'created'
UPDATED = 'updated'
RELATIONSHIPS = 'relationships'
INT_NODE_CREATED = 'int_node_created'
PROVIDED_PARENTS = 'provided_parents'
RELATIONSHIP_PROPS = 'relationship_properties'
BATCH_SIZE = 1000
ID_FUNC = 'ID'
#MASTER_NODE = False

pd.options.mode.chained_assignment = None


def get_btree_indexes(session):
    """
    Queries the database to get all existing indexes
    :param session: the current neo4j transaction session
    :return: A set of tuples representing all existing indexes in the database
    """
    command = "SHOW INDEXES"
    result = session.run(command)
    indexes = set()
    for r in result:
        if r["type"] == "BTREE":
            indexes.add(format_as_tuple(r["labelsOrTypes"][0], r["properties"]))
    return indexes


def format_as_tuple(node_name, properties):
    """
    Format index info as a tuple
    :param node_name: The name of the node type for the index
    :param properties: The list of node properties being used by the index
    :return: A tuple containing the index node_name followed by the index properties in alphabetical order
    """
    if isinstance(properties, str):
        properties = [properties]
    lst = [node_name] + sorted(properties)
    return tuple(lst)


def backup_neo4j(backup_dir, name, address, log):
    try:
        restore_cmd = 'To restore DB from backup (to remove any changes caused by current data loading, run ' \
                      'following commands:\n '
        restore_cmd += '#' * 160 + '\n'
        neo4j_cmd = 'neo4j-admin restore --from={}/{} --force'.format(backup_dir, name)
        mkdir_cmd = [
            'mkdir',
            '-p',
            backup_dir
        ]
        is_shell = False
        # settings for Windows platforms
        if platform.system() == "Windows":
            mkdir_cmd[2] = os.path.abspath(backup_dir)
            is_shell = True
        cmds = [
            mkdir_cmd,
            [
                'neo4j-admin',
                'backup',
                '--backup-dir={}'.format(backup_dir)
            ]
        ]
        if address in ['localhost', '127.0.0.1']:
            # On Windows, the Neo4j service cannot be accessed through the command line without an absolute path
            # or a custom installation location
            if platform.system() == "Windows":
                restore_cmd += '\tManually stop the Neo4j service\n\t$ {}\n\tManually start the Neo4j service\n'.format(
                    neo4j_cmd)
            else:
                restore_cmd += '\t$ neo4j stop && {} && neo4j start\n'.format(neo4j_cmd)
            for cmd in cmds:
                log.info(cmd)
                subprocess.call(cmd, shell=is_shell)
        else:
            second_cmd = 'sudo systemctl stop neo4j && {} && sudo systemctl start neo4j && exit'.format(neo4j_cmd)
            restore_cmd += '\t$ echo "{}" | ssh -t {} sudo su - neo4j\n'.format(second_cmd, address)
            for cmd in cmds:
                remote_cmd = ['ssh', address, '-o', 'StrictHostKeyChecking=no'] + cmd
                log.info(' '.join(remote_cmd))
                subprocess.call(remote_cmd)
        restore_cmd += '#' * 160
        return restore_cmd
    except Exception as e:
        log.exception(e)
        return False


def check_encoding(file_name):
    utf8 = 'utf-8'
    windows1252 = 'windows-1252'
    try:
        with open(file_name, encoding=utf8) as file:
            for _ in file.readlines():
                pass
        return utf8
    except UnicodeDecodeError:
        return windows1252


# Mask all relationship properties, so they won't participate in property comparison
def get_props_signature(props):
    clean_props = props
    for key in clean_props.keys():
        if '$' in key:
            clean_props[key] = ''
    signature = get_string_md5(str(clean_props))
    return signature


class DataLoader:
    def __init__(self, conn, memgraph, schema, database_name, conversion_files, plugins=None):
        if plugins is None:
            plugins = []
        if not schema or not isinstance(schema, ICDC_Schema):
            raise Exception('Invalid ICDC_Schema object')
        self.log = get_logger('Data Loader')
        self.conn = conn
        self.memgraph = memgraph
        self.schema = schema
        self.db_name = database_name
        self.rel_prop_delimiter = self.schema.rel_prop_delimiter
        self.wipe_timer = 0
        self.create_dict_timer = 0
        self.load_passed = 0
        self.load_node_time = 0
        self.update_dict_timer = 0
        self.relationship_passed = 0
        self.load_relation_time = 0
        self.cancer_fields = ["cancer_diagnosis_primary_site",
                              "cancer_diagnosis_disease_morphology",
                              "age_at_first_cancer_diagnosis"]

        if plugins:
            for plugin in plugins:
                if not hasattr(plugin, 'create_node'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'should_run'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_stat'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'nodes_created'):
                    raise ValueError('Invalid Plugin!')
                if not hasattr(plugin, 'relationships_created'):
                    raise ValueError('Invalid Plugin!')
        self.plugins = plugins
        self.nodes_created = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}

        # if no icd-O codes in file then convertsion file will be empty and not read this part
        if 'location' in conversion_files:
            self.schema.location_codes = get_cancer_translations_list(conversion_files["location"])
        if 'histology' in conversion_files:
            self.schema.histology_codes = get_cancer_translations(conversion_files["histology"])

    def get_schema_data(self, tx, query):
        result = tx.run(query)
        data_list = [i for i in result.data()]
        return data_list

    def get_data(self, tx, query, obj_data, batch_size):
        result = tx.run(query, batch=obj_data)
        return result.data()

    def get_schema_indexes(self, create_type):
        start = timer()
        self.node_keys_dict = dict()
        for result in self.memgraph.execute_and_fetch("SHOW INDEX INFO;"):
            self.node_keys_dict[result["label"]] = {'Primary ID': result["property"],
                                                    'Node_Index_DF':  pd.DataFrame(columns=['Node_ID', 'Primary_Key_Value'])}

        file_size = 0
        for curr_node in self.node_keys_dict:
            if self.node_keys_dict[curr_node]['Primary ID'] == 'None':
                continue
            query = f"MATCH (n:{curr_node}) RETURN {ID_FUNC}(n) as Node_ID, n.{self.node_keys_dict[curr_node]['Primary ID']} as Primary_Key_Value"
            cursor = self.conn.cursor()
            cursor.execute(query)
            records = cursor.fetchall()

            self.log.info(f"{curr_node} has {len(records)} nodes found in database")
            if len(records) > 0:
                data_res = pd.DataFrame(records)
                self.node_keys_dict[curr_node]['Node_Index_DF'] = pd.concat([self.node_keys_dict[curr_node]['Node_Index_DF'], data_res])
                file_size = file_size + len(data_res)

        self.log.info(' ')
        end = timer()
        self.create_dict_timer = end - start
        self.log.info('{0} Index Dictionary ({2}) nodes took: {1:.2f} seconds'.format(create_type, end - start, file_size))

    def update_indexs_with_new(self):
        dict_timer = timer()
        today = date.today()
        for curr_node in self.node_keys_dict:
            new_nodes = """MATCH (n:curr_node) where date(n.created) = """
            new_nodes += """date({year: """ + f"{today.year}, month: {today.month}, day: {today.day}"
            new_nodes += """}) RETURN """ + f"n.{self.node_keys_dict[curr_node]['Primary ID']} as Primary_Key_Value, {ID_FUNC}(n) as Node_ID"
            new_nodes = new_nodes.replace("curr_node", curr_node)
            
            cursor = self.conn.cursor()
            cursor.execute(new_nodes)
            records = cursor.fetchall()
                
            x = pd.concat([self.node_keys_dict[curr_node]['Node_Index_DF'], pd.DataFrame(records, columns =["Primary_Key_Value", "Node_ID"])])
            self.node_keys_dict[curr_node]['Node_Index_DF'] = x.drop_duplicates()
        self.update_dict_timer = timer() - dict_timer

    def check_files(self, file_list):
        if not file_list:
            self.log.error('Invalid file list')
            return False
        elif file_list:
            for data_file in file_list:
                if not os.path.isfile(data_file):
                    self.log.error('File "{}" does not exist'.format(data_file))
                    return False
            return True

    def check_column_names(self, file_name):
        #if file_name == "master_node":  # do not validate this file (Does not exist)
        #    return True
        self.log.info(f"preforming column wide validation for: {file_name}")
        curr_df = self.file_dict[file_name]
        # if "type" in curr_df:
        #    curr_df.drop("type", axis=1, inplace=True)
        if "type" not in curr_df:
            self.log.error(f"In {file_name} the column 'Type' is missing'")
            return False

        try:
            z = self.schema.get_props_for_node(file_name)
            schema_df = pd.DataFrame.from_dict(z, orient='index')
            schema_df.reset_index(inplace=True)
            schema_df.rename(columns={"index": "file_type"}, inplace=True)
            schema_df = schema_df[~schema_df["file_type"].str.contains("_original")]  # this is not part of the model
        #    schema_df = schema_df[~schema_df["file_type"].str.contains("_unit")]      # this is not part of the model
            # remove fields that point back to this node (no child)

            id_fields = self.schema.props.id_fields
            id_fields = pd.DataFrame([(i, f"{i}.{id_fields[i]}") for i in id_fields], columns=["file_name", "primary_key"])
            schema_df = schema_df[~schema_df["Type"].str.contains("""direction:IN""")]
            for curr_field in id_fields.index:
                x = schema_df[schema_df["Type"].str.contains(f"{id_fields.loc[curr_field]['file_name']}")]
                schema_df.loc[x.index, "file_type"] = id_fields.loc[curr_field]["primary_key"]
                schema_df.loc[x.index, "Type"] = "String"
        except Exception as e:
            print(e)

        input_colums = list(curr_df.columns)
        if "type" in input_colums:
            input_colums.remove("type")
        schema_cols = list(schema_df["file_type"])

        missing_from_schema = list(set(input_colums) - set(schema_cols))
        missing_from_file = list(set(schema_cols) - set(input_colums))

#        if len(missing_from_schema) > 0:
#            self.log.error(f"In {file_name} the following columns are in submission but not in schema: {missing_from_schema}")
        missing_from_file = [i for i in missing_from_file if "_unit" not in i]
        if len(missing_from_file) > 0:
            self.log.error(f"In {file_name} the following columns are in the schema but missing from submission: {missing_from_file}")
        if len(missing_from_file) == 0 and len(missing_from_schema) == 0:
            return True
        return False

    def validate_file_dict(self, cheat_mode, max_violations):
        if not cheat_mode:
            self.log.info('')
            self.log.info('Preforming data validation')
            validation_failed = False
            study_names = ["study not found"]
            index_files = []
            if "study" in self.file_dict:
                study_names = list(self.file_dict["study"]["study_short_name"])
            if "data_file" in self.file_dict:
                index_files = list(set(self.file_dict["data_file"]["study.study_short_name"]))
    
            study_names = list(set(study_names + index_files))
            
            for txt in self.file_dict:
                if not self.check_column_names(txt):
                    validation_failed = True
                if not self.validate_file(txt, max_violations, study_names):
                    self.log.error('Validating file "{}" failed!'.format(txt))
                    validation_failed = True
            return not validation_failed
        else:
            self.log.info('Cheat mode enabled, all validations skipped!')
            return True

    def get_neo4j_version(self, tx):
        # Use the dbms.components() procedure to get server details
        result = tx.run("CALL dbms.components() YIELD name, versions, edition "
                        "UNWIND versions AS version RETURN name, version, edition; ")
        data_list = [i for i in result.data()]
        #return result.single()
        return data_list[0]

    def create_data_dictionary(self, file_list):
        self.file_dict = {}
        for txt in file_list:
            try:
                if txt[:2] == "s3":  # files get loaded directly from S3
                    file_data = pd.read_csv(txt, sep='\t', header=0, encoding='windows-1252', storage_options={"profile": 'Popsci_Dev'})
                if txt[-3:] == "csv":
                    file_data = pd.read_csv(txt, encoding='windows-1252', keep_default_na=False)
                else:  # files are loaded from localf
                    file_data = pd.read_csv(txt, sep='\t', header=0, encoding='windows-1252', keep_default_na=False)
            except Exception as e:
                print(e)
                print(f"{txt} is empty")

            file_data.columns = [i.strip() for i in file_data.columns]
            try:
                file_data = file_data.query("type == type")  # remove blank lines in file
            except Exception as e:
                print(e)

            try:
                file_type = list(set(file_data["type"]))[0]
                if file_type not in self.file_dict.keys():
                    self.file_dict[file_type] = file_data
                else:
                    self.file_dict[file_type] = pd.concat([file_data, self.file_dict[file_type]])
            except Exception as e:
                print(e)

    def add_master_node(self):
        self.file_dict["master_node"] = pd.DataFrame(columns=["type", "desc", "node_name"])
        self.file_dict["master_node"].loc[0] = ["master_node", "master node to link all study nodes", "popsci_master"]
        if "study" in self.file_dict:
            self.file_dict["study"]["master_node.node_name"] = "popsci_master"

    def load(self, file_list, cheat_mode, dry_run, loading_mode, wipe_db, max_violations,
             split=False, no_backup=True, backup_folder="/", neo4j_uri=None):

        self.create_data_dictionary(file_list)

        """ add code here to validate the file dictionary """

        start = timer()
        if not self.validate_file_dict(cheat_mode, max_violations):
            return False
        
        #if MASTER_NODE:
        #    self.add_master_node()

        if not no_backup and not dry_run:
            if not neo4j_uri:
                self.log.error('No Neo4j URI specified for backup, abort loading!')
                sys.exit(1)
            backup_name = datetime.datetime.today().strftime(DATETIME_FORMAT)
            host = get_host(neo4j_uri)
            restore_cmd = backup_neo4j(backup_folder, backup_name, host, self.log)
            if not restore_cmd:
                self.log.error('Backup Neo4j failed, abort loading!')
                sys.exit(1)
        if dry_run:
            end = timer()
            self.log.info('Dry run mode, no nodes or relationships loaded.')  # Time in seconds, e.g. 5.38091952400282
            self.log.info('Running time: {:.2f} seconds'.format(end - start))  # Time in seconds, e.g. 5.38091952400282
            return {NODES_CREATED: 0, RELATIONSHIP_CREATED: 0}

        self.nodes_created = 0
        self.relationships_created = 0
        self.indexes_created = 0
        self.nodes_deleted = 0
        self.relationships_deleted = 0
        self.nodes_stat = {}
        self.relationships_stat = {}
        self.nodes_deleted_stat = {}
        self.relationships_deleted_stat = {}
        # Data updates and schema related updates cannot be performed in the same session so multiple will be created
        # Create new session for schema related updates (index creation)

        self.log.info("  ")
        self.log.info(f"Database name being used is: {self.db_name} ")
        self.log.info("  ")

       
        try:
            self.create_indexes()
        except Exception as e:
            self.log.exception(e)
            return False

        try:
            self._load_all(file_list, loading_mode, split, wipe_db)
        except Exception as e:
            self.log.exception(e)
            return False

        # End the timer
        end = timer()

        # Print statistics
        for plugin in self.plugins:
            combined_dict_counters(self.nodes_stat, plugin.nodes_stat)
            combined_dict_counters(self.relationships_stat, plugin.relationships_stat)
            self.nodes_created += plugin.nodes_created
            self.relationships_created += plugin.relationships_created
        return {NODES_CREATED: self.nodes_created, RELATIONSHIP_CREATED: self.relationships_created,
                NODES_DELETED: self.nodes_deleted, RELATIONSHIP_DELETED: self.relationships_deleted}

    def _load_all(self, file_list, loading_mode, split, wipe_db):
        try:
            if wipe_db:
                self.wipe_db()   # deletes all node in db
                self.create_indexes()
        except Exception as e:
            self.log.exception(e)
            return False

        self.load_passed = 0
        self.load_failed = 0
        self.relationship_passed = 0
        self.relationship_failed = 0

        # call create index after the wipe database (if was called)
        self.log.info(' ')
        self.log.info('Mapping index values to primary keys ')
        self.get_schema_indexes("Creating")   # create a dictionary that maps the current neo4j database as defined by self.db_name

        self.log.info(' ')
        processed_files = []
        load_start_time = time.perf_counter()

        try:
            for txt in self.file_dict:
                if loading_mode != DELETE_MODE:
                    new_nodes, updated_nodes, all_obj_list = self.load_nodes(txt, loading_mode, wipe_db, split)
                    processed_files.append(all_obj_list)
        except Exception as e:
            print(e)

        self.load_node_time = time.perf_counter()-load_start_time
        self.log.info(f"Number of Nodes Created / Updated: {self.load_passed}, Nodes Failed: {self.load_failed}")
        self.log.info(f"Total Loading time for all nodes: {self.load_node_time:.2f} seconds")

        self.log.info(' ')
        self.log.info('updating schema dictionary  for new nodes created')
        self.update_indexs_with_new()   # updates index dictionary with new nodes (created today)
        self.log.info('schema dictionary has been refreshed')  # Time in seconds, e.g. 5.38091952400282
        self.log.info(' ')

        rel_start_time = time.perf_counter()
        batch_size = 10000
        batch_index = 1

        for txt in processed_files:
            if loading_mode != DELETE_MODE:
                nodes_done = 0
                self.log.info("")
                self.log.info(f"making relationships for node: {txt[0]['type']}")
                while nodes_done < len(txt):
                    batch_time = time.time()
                    if len(txt) <= batch_size:
                        start_node = 0
                        end_node = len(txt)
                    elif (nodes_done+batch_size) > len(txt):
                        start_node = nodes_done
                        end_node = len(txt)
                    else:
                        start_node = nodes_done
                        end_node = nodes_done + batch_size
                    data_to_work = txt[start_node: end_node]

                    self.load_relationships(data_to_work, loading_mode, batch_index, batch_time,  split)
                    nodes_done += len(data_to_work)

                    batch_index += 1
#                    print(f"Total Elapsed time for relationships: {time.perf_counter()-batch_time:.4f} seconds")

        self.log.info(f"Total Number of Relationships Created / Updated: {self.relationship_passed}, " +
                      f"Nodes Failed: {self.relationship_failed}")

        self.load_relation_time = time.perf_counter()-rel_start_time
        self.log.info(f"Total time to make all relationships {self.load_relation_time:.2f} seconds")

    # Remove extra spaces at beginning and end of the keys and values
    @staticmethod
    def cleanup_node(node):
        return {key if not key else key.strip(): value if not value else value.strip()
                if isinstance(value, str) else value for key, value in node.items()}
        # return {key if not key else key.strip(): value if not value else value.strip() for key, value in node.items()}

    # Cleanup values for Boolean, Int and Float types
    # Add uuid to nodes if one not exists
    # Add parent id(s)
    # Add extra properties for "value with unit" properties
    def prepare_node(self, node):
        obj = self.cleanup_node(node)

        node_type = obj.get(NODE_TYPE, None)
        # Cleanup values for Boolean, Int and Float types
        if node_type:
            for key, value in obj.items():
                search_node_type = node_type
                search_key = key
                if is_parent_pointer(key):
                    search_node_type, search_key = key.split('.')
                elif self.schema.is_relationship_property(key):
                    search_node_type, search_key = key.split(self.rel_prop_delimiter)

                key_type = self.schema.get_prop_type(search_node_type, search_key)
                if key_type == 'Boolean':
                    cleaned_value = None
                    if isinstance(value, str):
                        if re.search(r'yes|true', value, re.IGNORECASE):
                            cleaned_value = True
                        elif re.search(r'no|false', value, re.IGNORECASE):
                            cleaned_value = False
                        else:
                            self.log.debug('Unsupported Boolean value: "{}"'.format(value))
                            cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Int':
                    try:
                        if value is None:
                            cleaned_value = None
                            cleaned_value = np.nan
                        else:
                            cleaned_value = int(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Float':
                    try:
                        if value is None:
                            cleaned_value = None
                        else:
                            cleaned_value = float(value)
                    except ValueError:
                        cleaned_value = None
                    obj[key] = cleaned_value
                elif key_type == 'Array':
                    items = get_list_values(value)

                    obj[key] = json.dumps(items)
                elif key_type == 'DateTime' or key_type == 'Date':
                    if value is None:
                        cleaned_value = None
                    else:
                        cleaned_value = reformat_date(value)
                    obj[key] = cleaned_value

        obj2 = {}
        for key, value in obj.items():
            obj2[key] = value
            # Add parent id field(s) into node
            if obj[NODE_TYPE] in self.schema.props.save_parent_id and is_parent_pointer(key):
                header = key.split('.')
                if len(header) > 2:
                    self.log.warning('Column header "{}" has multiple periods!'.format(key))
                field_name = header[1]
                parent = header[0]
                combined = '{}_{}'.format(parent, field_name)
                if field_name in obj:
                    self.log.debug(
                        '"{}" field is in both current node and parent "{}", use {} instead !'.format(key, parent,
                                                                                                      combined))
                    field_name = combined
                # Add an value for parent id
                obj2[field_name] = value
            # Add extra properties if any
            for extra_prop_name, extra_value in self.schema.get_extra_props(node_type, key, value).items():
                obj2[extra_prop_name] = extra_value

        if UUID not in obj2:
            id_field = self.schema.get_id_field(obj2)
            id_value = self.schema.get_id(obj2)
            node_type = obj2.get(NODE_TYPE)
            if node_type:
                try:
                    if not id_value:
                        obj2[UUID] = self.schema.get_uuid_for_node(node_type, self.get_signature(obj2))
                    elif id_field != UUID:
                        obj2[UUID] = self.schema.get_uuid_for_node(node_type, str(id_value))
                except Exception as e:
                    print(e)
            else:
                print('No "type" property in node')
        return obj2

    def get_signature(self, node):
        result = []
        for key in sorted(node.keys()):
            value = node[key]
            if not is_parent_pointer(key):
                result.append('{}: {}'.format(key, value))
        return '{{ {} }}'.format(', '.join(result))

    # Validate all cases exist in a data (TSV/TXT) file
    def validate_cases_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session(database=self.db_name) as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    obj = self.prepare_node(org_obj)
                    line_num += 1
                    # Validate parent exist
                    if CASE_ID in obj:
                        case_id = obj[CASE_ID]
                        if not self.node_exists(session, CASE_NODE, CASE_ID, case_id):
                            self.log.error(
                                'Invalid data at line {}: Parent (:{} {{ {}: "{}" }}) does not exist!'.format(
                                    line_num, CASE_NODE, CASE_ID, case_id))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                return not validation_failed

    # Validate all parents exist in a data (TSV/TXT) file
    def validate_parents_exist_in_file(self, file_name, max_violations):
        if not self.driver or not isinstance(self.driver, Driver):
            self.log.error('Invalid Neo4j Python Driver!')
            return False
        with self.driver.session(database=self.db_name) as session:
            file_encoding = check_encoding(file_name)
            with open(file_name, encoding=file_encoding) as in_file:
                self.log.info('Validating relationships in file "{}" ...'.format(file_name))
                reader = csv.DictReader(in_file, delimiter='\t')
                line_num = 1
                validation_failed = False
                violations = 0
                for org_obj in reader:
                    line_num += 1
                    obj = self.prepare_node(org_obj)
                    results = self.collect_relationships(obj, session, False, line_num)
                    relationships = results[RELATIONSHIPS]
                    provided_parents = results[PROVIDED_PARENTS]
                    if provided_parents > 0:
                        if len(relationships) == 0:
                            self.log.error('Invalid data at line {}: No parents found!'.format(line_num))
                            validation_failed = True
                            violations += 1
                            if violations >= max_violations:
                                return False
                    else:
                        self.log.info('Line: {} - No parents found'.format(line_num))

        return not validation_failed

    def get_node_properties(self, obj):
        """
        Generate a node with only node properties from input data
        :param obj: input data object (dict), may contain parent pointers, relationship properties etc.
        :return: an object (dict) that only contains properties on this node
        """
        node = {}

        for key, value in obj.items():
            if is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue
            else:
                node[key] = value

        return node

    # Validate the field names
    def validate_field_name(self, file_name):
        file_encoding = check_encoding(file_name)
        with open(file_name, encoding=file_encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')

            row = next(reader)
            row = self.cleanup_node(row)
            row_prepare_node = self.prepare_node(row)
            parent_pointer = []
            for key in row_prepare_node.keys():
                if is_parent_pointer(key):
                    parent_pointer.append(key)
            error_list = []
            parent_error_list = []
            for key in row.keys():
                if key not in parent_pointer:
                    try:
                        if key not in self.schema.get_props_for_node(row['type']) and key != 'type':
                            error_list.append(key)
                    except Exception:
                        error_list.append(key)
                else:
                    try:
                        if key.split('.')[1] not in self.schema.get_props_for_node(key.split('.')[0]):
                            parent_error_list.append(key)
                    except Exception:
                        parent_error_list.append(key)
            if len(error_list) > 0:
                for error_field_name in error_list:
                    self.log.warning('Property: "{}" not found in data model'.format(error_field_name))
            if len(parent_error_list) > 0:
                for parent_error_field_name in parent_error_list:
                    self.log.error('Parent pointer: "{}" not found in data model'.format(parent_error_field_name))
                self.log.error('Parent pointer not found in the data model, abort loading!')
                return False
        return True

    # Validate file
    def validate_file(self, file_name, max_violations, study_names):
        if file_name == "master_node":
            return True
        self.log.info(f"preforming data level validation for: {file_name}")
        df = self.file_dict[file_name]
        df = df.astype(str)     # make everything strings, even numbers
        violations = 0
        obj = df.iloc[0].to_dict()
        properties = self.schema.nodes[obj[NODE_TYPE]][PROPERTIES]
        properties["study.study_short_name"] = {"Type":"String", "Req":"Yes", "enum": set(study_names)}
        no_cancer = []

        for curr_field in df.columns:
            need_to_check = True
            if curr_field in ["type", "number_of_participants"]:
                need_to_check = False   # type is not in the model but is part of the submitted files
            elif curr_field in properties:
                if "@relation" in properties[curr_field]["Type"]:
                    need_to_check = False  # this is a relation variable and not validated here

                
            #    need_to_check = False   # type is not in the model but is part of the submitted files
            elif len(curr_field.split('.')) > 1:
                need_to_check = False  # this is a relationship column, value depends on another file
           
            if need_to_check:
                valid_list = []
                try:
                    if curr_field == "cancer_diagnosis_primary_site":
                        valid_list = list(self.schema.location_codes["ICD-O-3 Code"])
                    elif curr_field == "cancer_diagnosis_disease_morphology":
                        valid_list = list(self.schema.histology_codes["ICD-O-3 Code"])
                    elif "enum" in properties[curr_field]:
                        valid_list = properties[curr_field]["enum"]
                    elif "item_type" in properties[curr_field]:
                        if "enum" in properties[curr_field]["item_type"]:
                            valid_list = properties[curr_field]["item_type"]["enum"]
                        else:
                            print("item type no enum")
                except Exception as e:
                    print(e)

                if "minimum" in properties[curr_field]:
                    min_val = float(properties[curr_field]["minimum"])
                    max_val = float(properties[curr_field]["maximum"])
                    has_val = df.query("{0} == {0}".format(curr_field))
                    has_val[curr_field] = has_val[curr_field].astype(float)
                    out_of_range = has_val.query("{0} < {1} or {0} > {2}".format(curr_field, min_val, max_val))
                    if len(out_of_range) > 0:
                        self.log.error(f'{curr_field} has {len(out_of_range)} values outside the acceptable range: [{min_val}, {max_val}]')
                if "Req" in properties[curr_field] and curr_field not in self.cancer_fields:
                    if properties[curr_field]["Req"] == "Yes":
                        check_blank = df.query("`{0}` != `{0}` or `{0}` == 'nan'".format(curr_field))
                        if len(check_blank) > 0:
                            violations += 1
                            self.log.error(f'Required property: "{curr_field}" is empty!')
                    else:
                        check_blank = df.query("{0} != {0}".format(curr_field))
                        if len(check_blank) > 0:
                            self.log.warning(f'property is blank: "{curr_field}" is empty!')
                if curr_field in self.cancer_fields:
                    no_cancer = df.query("participant_case_indicator != 'Yes'")
                    test_df = df.query("participant_case_indicator == 'Yes'")
                else:
                    test_df = df.copy()

                if len(valid_list) > 0:
                    # allow for piked entries to be validated correctly
                    test_df[curr_field] = [i.split("|") for i in test_df[curr_field]]
                    test_df = test_df.explode(curr_field)
                    test_df[curr_field] = [i.strip() for i in test_df[curr_field]]

                    error_data = list(set(list(test_df[curr_field])) - set(valid_list))
                    if len(error_data) > 0:
                        if curr_field in self.cancer_fields:
                            self.log.error(f"In {curr_field}: errors were found: {error_data}.")
                            print(test_df.query(f"{curr_field} in {error_data}"))
                        else:
                            self.log.error(f"In {curr_field}: errors were found: {error_data}.  valid values are: {valid_list}")
                            print(test_df.query(f"{curr_field} in {error_data}"))
                        violations += 1
                if len(no_cancer) > 0 and curr_field in self.cancer_fields:
                    error_data = no_cancer[no_cancer[curr_field] != 'nan']
                    if len(error_data) > 0:
                        self.log.error(f"In {curr_field}: participant is listed as participant_case_indicator == 'No' " +
                                       ", but a value was found (expected blank)")
                        violations += 1

        if violations == 0:
            return True
        return False

    def get_new_statement(self, node_type, obj):
        # statement is used to create current node
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('{0}: ${0}'.format(key))

        statement = 'CREATE (:{0} {{ {1} }})'.format(node_type, ' ,'.join(prop_stmts))
        return statement

    def get_upsert_statement(self, node_type, id_field, obj):
        # statement is used to create current node
        statement = ''
        prop_stmts = []

        for key in obj.keys():
            if key in excluded_fields:
                continue
            elif key == id_field:
                continue
            elif is_parent_pointer(key):
                continue
            elif self.schema.is_relationship_property(key):
                continue

            prop_stmts.append('n.{0} = ${0}'.format(key))

        statement += 'MERGE (n:{0} {{ {1}: ${1} }})'.format(node_type, id_field)
        statement += ' ON CREATE ' + 'SET n.{} = datetime(), '.format(CREATED) + ' ,'.join(prop_stmts)
        statement += ' ON MATCH ' + 'SET n.{} = datetime(), '.format(UPDATED) + ' ,'.join(prop_stmts)
        return statement

    # Delete a node and children with no other parents recursively
    def delete_node(self, session, node):
        delete_queue = deque([node])
        node_deleted = 0
        relationship_deleted = 0
        while len(delete_queue) > 0:
            root = delete_queue.popleft()
            delete_queue.extend(self.get_children_with_single_parent(session, root))
            n_deleted, r_deleted = self.delete_single_node(session, root)
            node_deleted += n_deleted
            relationship_deleted += r_deleted
        return node_deleted, relationship_deleted

    # Return children of node without other parents
    def get_children_with_single_parent(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }})<--(m)'.format(node_type, self.schema.get_id_field(node))
        statement += ' WHERE NOT (n)<--(m)-->() RETURN m'
        result = session.run(statement, node)
        children = []
        for obj in result:
            children.append(self.get_node_from_result(obj, 'm'))
        return children

    @staticmethod
    def get_node_from_result(record, name):
        node = record.data()[name]
        result = dict(node.items())
        for label in record[0].labels:
            result[NODE_TYPE] = label
            break
        return result

    # Simple delete given node, and it's relationships
    def delete_single_node(self, session, node):
        node_type = node[NODE_TYPE]
        statement = 'MATCH (n:{0} {{ {1}: ${1} }}) detach delete n'.format(node_type, self.schema.get_id_field(node))
        result = session.run(statement, node)
        nodes_deleted = result.consume().counters.nodes_deleted
        self.nodes_deleted += nodes_deleted
        self.nodes_deleted_stat[node_type] = self.nodes_deleted_stat.get(node_type, 0) + nodes_deleted
        relationship_deleted = result.consume().counters.relationships_deleted
        self.relationships_deleted += relationship_deleted
        return nodes_deleted, relationship_deleted

    # load file
    def convert_df_to_dict(self, file_data):
        total_records = len(file_data)
        file_data.columns = [i if len(i.split('.')) == 1 else i.split('.')[1] for i in file_data.columns]
        qry_str = ['n.' + i + ' = row.' + i for i in file_data.columns]
        file_data = file_data.to_dict('records')

        return file_data, qry_str, total_records

    def process_data_in_batches(self, col_list, data, create_type, node_type):
      #  statement += ' ON CREATE ' + 'SET n.{} = datetime(), '.format(CREATED) + ' ,'.join(prop_stmts)
      #  statement += ' ON MATCH ' + 'SET n.{} = datetime(), '.format(UPDATED) 
        
        curr_time = datetime.datetime.now()
        formatted_time = curr_time.strftime("%Y-%m-%dT%H:%M:%S")
        if create_type == "CREATE":
            var_list = [f"{i}: node_data.{i}" for i in col_list]
            var_list = var_list + [CREATED + f": localDateTime('{formatted_time}')"]
        else:
            var_list = [f"u.{i}= ${i}" for i in col_list]
            var_list = var_list + [UPDATED + ": " + str(datetime.datetime.now())]
        var_list = ", ".join(var_list)
        
        if create_type == "CREATE":
            query = """UNWIND $data AS node_data
            CREATE (n:new_node {var_list}) """
        
        else:
            query = """ UNWIND $data AS node_data
                        MATCH (u:update_node {col_list: data.col_list})
                        Set var_list"""
                           
        query = query.replace("var_list", str(var_list))
        query = query.replace("new_node", node_type)
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, {"data": data})
            self.conn.commit()
        except Exception as e:
            print(e)


    def process_relationships_batches(self, tx, data, curr_qry):
        new_qry = """CALL apoc.periodic.iterate( """
        new_qry += """\"UNWIND $data AS batch return batch\", """  # Iterate statement: Unwinds the list of data items
        new_qry += """ \"old_qry \", """

        new_qry += """{batchSize: 10000, """   # Process 1000 items per batch
        new_qry += """parallel: true, """     # Run batches sequentially
        new_qry += """iterateList: true, """  # process all batches at once
        new_qry += """params: { data: $data } } )"""   # Pass the data list as a parameter

        new_qry = new_qry.replace("old_qry", curr_qry)

        result = tx.run(new_qry, data=data)
        # result = tx.run(curr_qry, data)

        data_list = [i for i in result.data()]
        return data_list[0]

    def write_nodes_to_db(self, col_name, node_type, current_nodes, create_type):

        current_nodes.columns = [i if len(i.split('.')) == 1 else i.split('.')[1] for i in current_nodes.columns]
        col_list = current_nodes.columns
        file_data, qry_str, total_records = self.convert_df_to_dict(current_nodes)
        rem_list = ['Node_Exists', 'Node_ID', 'Primary_Key_Value'] + [i for i in col_list if "Unnamed" in i]
        col_list = [i for i in col_list if "Unnamed" not in i]

        index = 0
        for curr_row in file_data:
            file_data[index] = {i: curr_row[i] for i in curr_row if i not in rem_list}
            index += 1

        batch_size = 10000  # Adjust based on your dataset size and Memgraph resources
        done_nodes = 0
        while done_nodes < len(file_data):
            batch = file_data[done_nodes:(done_nodes+batch_size)]
            self.process_data_in_batches(col_list, batch, create_type, node_type)

            done_nodes += len(batch)        
            self.load_passed += len(batch)
            self.load_failed += 0

    def load_nodes(self, file_name, loading_mode,  wipe_db, split=False):
        if loading_mode == NEW_MODE:
            action_word = 'Loading new'
        elif loading_mode == UPSERT_MODE:
            action_word = 'Loading'
        elif loading_mode == DELETE_MODE:
            action_word = 'Deleting'
        else:
            raise Exception('Wrong loading_mode: {}'.format(loading_mode))
        self.log.info(' ')
        self.log.info('{} nodes from file: {}'.format(action_word, file_name))
        
     
        # file_data = pd.read_csv(file_name, sep='\t', header=0)
        file_data = self.file_dict[file_name]
        for col in file_data:
            if file_data[col].dtype in ['int64', 'float64']:
                file_data[col] = file_data[col].fillna(np.nan)
            elif file_data[col].dtype == 'datetime64[ns]':
                file_data[col] = file_data[col].fillna(pd.to_datetime('1900-01-01'))
            else:
                file_data[col] = file_data[col].fillna("missing data")

        # terms would only exist if conversion files are present, else will ignore
#        if 'location_codes' in dir(self.schema):
#            file_data = convert_codes(file_data, 'cancer_diagnosis_primary_site', self.schema.location_codes)
#        if 'histology_codes' in dir(self.schema):
#            file_data = convert_codes(file_data, 'cancer_diagnosis_disease_morphology', self.schema.histology_codes)

        # load nodes in batches of 5,000
        nodes_done = 0
        all_obj = []
        all_new_nodes = 0
        all_existing_nodes = 0
        batches = 0

        # remove duplicate records by primay ID (This value should be unique)
        try:
            key_name = self.schema.get_id_field(file_data.iloc[0].to_dict())
            file_data = file_data.drop_duplicates(key_name)
            file_data = file_data.query(f"{key_name} != 'missing data'")
        except Exception as e:
            print(e)

        while nodes_done < len(file_data):
            batch_size = 5000
            batch_time = time.time()
            if len(file_data) <= batch_size:
                start_node = 0
                end_node = len(file_data)
            elif (nodes_done+batch_size) > len(file_data):
                start_node = nodes_done
                end_node = len(file_data)
            else:
                start_node = nodes_done
                end_node = nodes_done + batch_size
            data_to_work = file_data[start_node: end_node]

            data_to_work.reset_index(inplace=True, drop=True)
            for index in data_to_work.index:
                obj = self.prepare_node(data_to_work.iloc[index].to_dict())  # formats incomming data
                all_obj.append(obj)

            df = pd.DataFrame(all_obj[start_node: end_node])
            col_name = self.schema.get_id_field(obj)  # primary key field
            node_type = obj[NODE_TYPE]   # current node to match

            if loading_mode != DELETE_MODE:
                check_db = df.merge(self.node_keys_dict[node_type]['Node_Index_DF'],
                                    left_on=self.schema.get_id_field(obj), right_on="Primary_Key_Value",
                                    how="left", indicator="Node_Exists")

                # if the node does not already exist on primary key then create it
                new_nodes = check_db.query("Node_Exists == 'left_only'")   # use create
                if len(new_nodes) > 0:
                    self.write_nodes_to_db(col_name, node_type, new_nodes, "CREATE")

                # if the node does  already exist on primary key then update it using the merge statement
                existing_nodes = check_db.query("Node_Exists == 'both'")   # use match
                if len(existing_nodes) > 0:
                    self.write_nodes_to_db(col_name, node_type, existing_nodes, "MATCH")
            all_existing_nodes += len(existing_nodes)
            all_new_nodes += len(new_nodes)

            nodes_done += len(data_to_work)
            batches += 1
            end_time = time.time()
            self.log.info(f"Completed batch {batches}, total nodes done: {nodes_done} in {end_time - batch_time: .2f} seconds")
        return all_new_nodes, all_existing_nodes, all_obj

    def node_exists(self, session, label, prop, value):
        statement = 'MATCH (m:{0} {{ {1}: ''${1}'' }}) return m'.format(label, prop)
        result = session.run(statement, {prop: value})
        count = len(result.data())
        if count > 1:
            self.log.warning('More than one nodes found! ')
        return count >= 1

    def collect_relationships(self, obj, session, create_intermediate_node, line_num):
        node_type = obj[NODE_TYPE]
        relationships = []
        int_node_created = 0
        provided_parents = 0
        relationship_properties = {}
        for key, value in obj.items():
            if is_parent_pointer(key):
                provided_parents += 1
                other_node, other_id = key.split('.')
                relationship = self.schema.get_relationship(node_type, other_node)
                if not isinstance(relationship, dict):
                    self.log.error('Line: {}: Relationship not found!'.format(line_num))
                    raise Exception('Undefined relationship, abort loading!')
                relationship_name = relationship[RELATIONSHIP_TYPE]
                multiplier = relationship[MULTIPLIER]
                if not relationship_name:
                    self.log.error('Line: {}: Relationship not found!'.format(line_num))
                    raise Exception('Undefined relationship, abort loading!')
                if not self.node_exists(session, other_node, other_id, value):
                    create_parent = False
                    if create_intermediate_node:
                        for plugin in self.plugins:
                            if plugin.should_run(other_node, MISSING_PARENT):
                                create_parent = True
                                if plugin.create_node(session, line_num, other_node, value, obj):
                                    int_node_created += 1
                                    relationships.append(
                                        {PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                         RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
                                else:
                                    self.log.error(
                                        'Line: {}: Could not create {} node automatically!'.format(line_num,
                                                                                                   other_node))
                    else:
                        self.log.warning(
                            'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                              other_id,
                                                                                              value))
                    if not create_parent:
                        self.log.warning(
                            'Line: {}: Parent node (:{} {{{}: "{}"}} not found in DB!'.format(line_num, other_node,
                                                                                              other_id,
                                                                                              value))
                else:
                    if multiplier == ONE_TO_ONE and self.parent_already_has_child(session, node_type, obj,
                                                                                  relationship_name, other_node,
                                                                                  other_id, value):
                        self.log.error(
                            'Line: {}: one_to_one relationship failed, parent already has a child!'.format(line_num))
                    else:
                        relationships.append({PARENT_TYPE: other_node, PARENT_ID_FIELD: other_id, PARENT_ID: value,
                                              RELATIONSHIP_TYPE: relationship_name, MULTIPLIER: multiplier})
            elif self.schema.is_relationship_property(key):
                rel_name, prop_name = key.split(self.rel_prop_delimiter)
                if rel_name not in relationship_properties:
                    relationship_properties[rel_name] = {}
                relationship_properties[rel_name][prop_name] = value
        return {RELATIONSHIPS: relationships, INT_NODE_CREATED: int_node_created, PROVIDED_PARENTS: provided_parents,
                RELATIONSHIP_PROPS: relationship_properties}

    def parent_already_has_child(self, session, node_type, node, relationship_name, parent_type, parent_id_field,
                                 parent_id):
        statement = 'MATCH (n:{})-[r:{}]->(m:{} {{ {}: $parent_id }}) return n'.format(node_type, relationship_name,
                                                                                       parent_type, parent_id_field)
        result = session.run(statement, {"parent_id": parent_id})
        if result:
            child = result.single()
            if child:
                find_current_node_statement = 'MATCH (n:{0} {{ {1}: ${1} }}) return n'.format(node_type,
                                                                                              self.schema.get_id_field(
                                                                                                  node))
                current_node_result = session.run(find_current_node_statement, node)
                if current_node_result:
                    current_node = current_node_result.single()
                    return child[0].id != current_node[0].id
                else:
                    self.log.error('Could NOT find current node!')

        return False

    # Check if a relationship of same type exists, if so, return a statement which can delete it, otherwise return False
    def has_existing_relationship(self, session, node_type, node, relationship, curr_index,  count_same_parent=False):
        relationship_name = relationship[RELATIONSHIP_TYPE]
        parent_type = relationship[PARENT_TYPE]
        parent_id_field = relationship[PARENT_ID_FIELD]

        base_statement = 'MATCH (n:{0})-[r:{2}]->(m:{3}) where n.{1} = ${1} and {5}(n) = {4} '.format(node_type,
                                                                                                      self.schema.get_id_field(node),
                                                                                                      relationship_name, parent_type,
                                                                                                      curr_index, ID_FUNC)
        statement = base_statement + ' return m.{} AS {}'.format(parent_id_field, PARENT_ID)
        result = session.run(statement, node)

        if result:
            old_parent = result.single()
            if old_parent:
                if count_same_parent:
                    del_statement = base_statement + ' delete r'
                    return del_statement
                else:
                    old_parent_id = old_parent[PARENT_ID]
                    if old_parent_id != relationship[PARENT_ID]:
                        self.log.warning('Old parent is different from new parent, delete relationship to old parent:'
                                         + ' (:{} {{ {}: "{}" }})!'.format(parent_type, parent_id_field, old_parent_id))
                        del_statement = base_statement + ' delete r'
                        return del_statement
        else:
            self.log.error('Remove old relationship failed: Query old relationship failed!')

        return False

    def remove_old_relationship(self, session, node_type, node, relationship, curr_index):
        del_statement = self.has_existing_relationship(session, node_type, node, relationship, curr_index)
        if del_statement:
            del_result = session.run(del_statement, node)
            if not del_result:
                self.log.error('Delete old relationship failed!')

    def load_relationships(self, file_data, loading_mode, batch_index, batch_time,  split=False):
        curr_time = datetime.datetime.now()
        formatted_time = curr_time.strftime("%Y-%m-%dT%H:%M:%S")
        new_date = f"localDateTime('{formatted_time}')"
        
        file_data_df = pd.DataFrame(file_data)
        obj = file_data_df.iloc[0].to_dict()

        node_type = obj[NODE_TYPE]
        file_data_df = file_data_df.merge(self.node_keys_dict[node_type]['Node_Index_DF'],
                                          left_on=self.schema.get_id_field(obj), right_on="Primary_Key_Value",
                                          how="left", indicator="Node_Exists")

        missing_id = file_data_df.query("Node_Exists not in ['both']")
        if len(missing_id) > 0:
            self.log.error(f"Node that failed is {node_type}")
            self.log.error(f"{missing_id}")
            self.log.error("Unable to make relationships: file data does not align properly with database")

        file_data_df.drop("Node_Exists", axis=1, inplace=True)
        ids = self.schema.props.id_fields
        header_names = [f"{curr_id}.{ids[curr_id]}" for curr_id in ids]

        check_relationship = [i for i in file_data_df.columns if i in header_names]
        if len(check_relationship) == 0:
            self.log.info(f"no relationship found for file type: {list(set(file_data_df[NODE_TYPE]))}")
        else:
            for curr_relationship in check_relationship:
                try:
                    #if node_type == "study" and MASTER_NODE:
                    #    self.schema.relationships["study"] = {'master_node': {'relationship_type': 'belongs_to', 'Mul': 'many_to_one'}}
                    
                    try:
                        relation = self.schema.relationships[node_type][curr_relationship.split('.')[0]]['relationship_type']
                    except Exception as e:
                        print(e)
                        
                    print(f"parent_node: {curr_relationship.split('.')[0]} is being mapped to  child_node: {node_type}")
                    
                    qry_str = """UNWIND $file_data_dct AS batch """
                    qry_str += f"MATCH (m:{curr_relationship.split('.')[0]}) "
                    qry_str += f"MATCH (n:{node_type}) "
                    qry_str += f"where m.{curr_relationship.split('.')[1]} = batch.{curr_relationship.split('.')[1]} and "

                    qry_str += 'n.{0} = batch.{0} and {1}(n) = batch.Node_ID '.format(self.schema.get_id_field(obj), ID_FUNC)
                    qry_str += f"MERGE (n)-[r:{relation}]->(m) ON CREATE SET r.created = datetime ON MATCH SET r.updated = datetime"
                    qry_str += "return r"

                    file_data_df.columns = [i if len(i.split('.')) == 1 else i.split('.')[1] for i in file_data_df.columns]
                    file_data_dct = file_data_df.to_dict('records')
                    
                    qry_str = qry_str.replace("datetime", new_date)  

                    cursor = self.conn.cursor()
                    cursor.execute(qry_str, {"file_data_dct": file_data_dct})
                    qry_result = cursor.fetchall()
                    self.conn.commit()

                    rel_created = len(qry_result)

                    end_time = time.time()
                    self.log.info(f"Completed batch {batch_index}, Relationships Created: {rel_created}, " +
                                  f"Relationships Failed: {len(file_data) - rel_created} in {end_time - batch_time: .2f} seconds")

                    self.relationship_passed += rel_created
                    self.relationship_failed += len(file_data) - rel_created

                except Exception as e:
                    print(e)
                    print(file_data_dct)
        return True

    @staticmethod
    def get_relationship_prop_statements(props):
        prop_stmts = []

        for key in props:
            prop_stmts.append('r.{0} = ${0}'.format(key))
        return prop_stmts

    def clean_database(self, tx):
        batch_size = 10000
        self.log.info(" ")

        query = """
             CALL apoc.periodic.iterate(
            "MATCH (n) RETURN n",
            "DETACH DELETE n",
            {batchSize: $batch_size, parallel: true} ) """
        result = tx.run(query, batch_size=batch_size)

        data_list = [i for i in result.data()]
        return data_list[0]

    def get_current_indxes(self, tx):
        command = "SHOW INDEXES"
        result = tx.run(command)
        data_list = [i for i in result.data()]
        return data_list

    def drop_old_indexes(self, tx, query):
        tx.run(query)

    def wipe_db(self, split=False):
        wipe_timer = time.perf_counter()
        self.log.info('In process of wipping Database...')
        cursor = self.conn.cursor()
        
        cursor.execute("MATCH (n) return count(n)")
        self.conn.commit()
        
        total_nodes = cursor.fetchall()
        total_nodes = total_nodes[0][0]
        deleted_nodes = 0
        self.log.info(f"there are {total_nodes} that need to be removed")
        
        while deleted_nodes < total_nodes:
            query = """ MATCH (n) WITH n LIMIT 10000 DETACH DELETE n
                        RETURN count(n) AS nodesDeleted """
            
            cursor.execute(query)
            results = cursor.fetchall()
            deleted_nodes += results[0][0]
            self.conn.commit()
            self.log.info(f"deleted {deleted_nodes} so far")

        self.log.info(" ")
        self.wipe_timer = time.perf_counter() - wipe_timer
        self.log.info(f'{deleted_nodes} nodes deleted in {self.wipe_timer:.2f}')
        
        query = """CALL schema.assert({}, {}, {}, true) 
                    YIELD action, key, keys, label, unique
                    RETURN action, key, keys, label, unique;"""
        
        self.memgraph.execute(query)
        self.log.info(' indexes have ben dropped')
       
    def wipe_db_split(self, session):
        while True:
            tx = session.begin_transaction()
            try:
                cleanup_db = f'MATCH (n) WITH n LIMIT {BATCH_SIZE} DETACH DELETE n'
                result = tx.run(cleanup_db).consume()
                tx.commit()
                deleted_nodes = result.counters.nodes_deleted
                self.nodes_deleted += deleted_nodes
                deleted_relationships = result.counters.relationships_deleted
                self.relationships_deleted += deleted_relationships
                self.log.info(f'{deleted_nodes} nodes deleted...')
                self.log.info(f'{deleted_relationships} relationships deleted...')
                if deleted_nodes == 0 and deleted_relationships == 0:
                    break
            except Exception as e:
                tx.rollback()
                self.log.exception(e)
                raise e
        self.log.info('{} nodes deleted!'.format(self.nodes_deleted))
        self.log.info('{} relationships deleted!'.format(self.relationships_deleted))

    def create_indexes(self):
        """
        Creates indexes, if they do not already exist, for all entries in the "id_fields" and "indexes" sections of the
        properties file
        :param session: the current neo4j transaction session
        """
        ids = self.schema.props.id_fields
        curr_indexes = self.memgraph.get_indexes()
        index_df = pd.DataFrame(curr_indexes)
        
        for node_name in ids:
            self.create_index(node_name, ids[node_name], index_df)
        indexes = self.schema.props.indexes
        for node_dict in indexes:
            node_name = list(node_dict.keys())[0]
            self.create_index(node_name, node_dict[node_name], index_df)

    def create_index(self, node_name, node_property, index_df):
        if len(index_df) > 0:  #if indexes exist check againt nodes
            if len(index_df.query(f"label == '{node_name}' and property == '{node_property}'")) > 0:
                return   #index already exists, do not make again
        
        query = f"CREATE INDEX ON :{node_name}({node_property});"
        self.memgraph.execute(query)

        self.indexes_created += 1
        self.log.info("Index created for \"{}\" on property \"{}\"".format(node_name, node_property))