import yaml
import pandas as pd
import requests


def get_cancer_translations_list(yml_file):

    with open(yml_file) as f:
        cancer_term_file = yaml.load(f, Loader=yaml.FullLoader)

    all_terms = pd.DataFrame()
    for curr_key in cancer_term_file.keys():
        for curr_location in cancer_term_file[curr_key]:
            try:
                x = pd.DataFrame.from_dict(cancer_term_file[curr_key][curr_location])  # , orient='index')
                x["ICD-O-3 Code"] = curr_location
                x.reset_index(inplace=True)

                all_terms = pd.concat([all_terms, x])
            except Exception as e:
                print(e)

    # all_terms.columns = ["Sub Site", "ICD-O-3 Code", "Primary Site"]
    return all_terms


def get_cancer_translations(yml_file):
   # property_value_code = '13606049'
   # URL = f'https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElements?publicId=13606049'
   # headers = {'Accept': 'application/json'}
   # response = requests.get(URL, headers=headers)
   # data = response.json()
   # values = data["DataElements"][0]["ValueDomain"]["PermissibleValues"]
   # allowed_values = [i["value"] for i in values]
   # sub_longname = [i["ValueMeaning"]["longName"] for i in values]
    
    with open(yml_file) as f:
        cancer_term_file = yaml.load(f, Loader=yaml.FullLoader)

    all_terms = pd.DataFrame()
    for curr_key in cancer_term_file.keys():
        x = pd.DataFrame.from_dict(cancer_term_file[curr_key], orient='index')
        #  x["Primary Site"] = None
        x.reset_index(inplace=True)
        all_terms = pd.concat([all_terms, x])

    all_terms.columns = ["ICD-O-3 Code", "VM Long Name"]
    return all_terms


def convert_codes(df, column_name, code_list):
    org_cols = df.columns
    if column_name in df.columns:
        org_cols = org_cols.drop(column_name)
        df[column_name] = [i.split("|") for i in df[column_name]]
        df = df.explode(column_name)
        
        df = df.merge(code_list, left_on=column_name, right_on="ICD-O-3 Code", how="left")

        df.drop(column_name, axis=1, inplace=True)
        # df.rename(columns={"Sub Site": column_name, "ICD-O-3 Code": "ICD-O-3 Code" + column_name.replace("cancer_diagnosis", "")}, inplace=True)
        df.rename(columns={"ICD-O-3 Code": "ICD-O-3 Code" + column_name.replace("cancer_diagnosis", "")}, inplace=True)
        df.rename(columns={"VM Long Name": column_name, "UBERON Preferred Term": column_name}, inplace=True)
        if 'index' in df.columns:
            df.drop("index", axis=1, inplace=True)

        x = df.query("participant_case_indicator == 'No'")
        df.loc[x.index, column_name] = "N/A"
        df.loc[x.index, "ICD-O-3 Code" + column_name.replace("cancer_diagnosis", "")] = "N/A"
        df.loc[x.index, "NCIt Concept Code"] = "Not Applicable"
        df.loc[x.index, "NCIt Preferred Term"] = "Not Applicable"
        df.loc[x.index, "cancer_diagnosis_primary_site"] = "Not Applicable"
        df.loc[x.index, "cancer_diagnosis_disease_morphology"] = "Not Applicable"
        
        cols_to_agg = [i for i in df.columns if i not in org_cols]
        agg_dict = {i : lambda x: ' ^ '.join(x) for i in cols_to_agg}
        df = df.groupby(list(org_cols)).agg(agg_dict)
        df.reset_index(inplace=True)

    return df
