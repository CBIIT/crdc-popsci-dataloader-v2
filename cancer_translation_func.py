import yaml
import pandas as pd


def get_cancer_translations_list(yml_file):
    with open(yml_file) as f:
        cancer_term_file = yaml.load(f, Loader=yaml.FullLoader)

    all_terms = pd.DataFrame()
    for curr_key in cancer_term_file.keys():
        for curr_location in cancer_term_file[curr_key]:
            x = pd.DataFrame.from_dict(cancer_term_file[curr_key][curr_location])  # , orient='index')
            x["ICD-O-3 Code"] = curr_location
            x.reset_index(inplace=True)

            all_terms = pd.concat([all_terms, x])

    # all_terms.columns = ["Sub Site", "ICD-O-3 Code", "Primary Site"]
    return all_terms


def get_cancer_translations(yml_file):
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
    if column_name in df.columns:
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
    return df
