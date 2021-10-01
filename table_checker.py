import pyodbc
import getpass
import pandas as pd
import os
import datetime
import re


def connect():
    '''
    input server connections and login details
    '''
    #server = input("Input server name: ")
    server = "SERPSQL"
    database = "UKSERPUKLLC" # temp
    #uid = input("Input user id: ")
    #pwd = getpass.getpass(prompt="Input password: ")

    '''
    cnxn_str = "DRIVER={}; SERVER={}; DATABASE={}; UID={}; PWD={}".format(
        "{SQL Server Native Client 11.0}",
        server,
        database,
        uid,
        pwd
    )
    '''
    cnxn_str ="DRIVER={}; SERVER={}; DATABASE={}; Trusted_Connection=yes".format(
        "{SQL Server Native Client 11.0}",
        server,
        database
    )

    cnxn = pyodbc.connect(cnxn_str)
    return cnxn

def verify_date_format(date):
    '''
    Non-valid Date formats (must be DD/MM/YYYY)
    '''
    date_format = '%d/%m/%Y'
    try:
        date_obj = datetime.strptime(date, date_format)
        return True
    except ValueError:
        return False

def verify_version(version):
    pattern = re.compile("^v[0-9]{4}")
    if pattern.match(version):
        return True
    else:
        return False


def name_validator(df):
    parts = df["table_name"].split("_")
    # Contains at least 3 underscore separated sections
    if len(parts) < 3:
        return "No"
    
    # Final section is YYYYMMDD
    if verify_date_format(parts[-1]):
        # Section contains version of form v0001
        if verify_version(parts[-2]):
            # section is version, version and -description, or version and -values
            if len(parts[-2] == 5) or (len(parts[-2]) == 17 and "-description" in parts[-2]) or (len(parts[-2]) == 12 and "-values" in parts[-2]):
                return True
    # if any one condition fails
    return False

def verify_table_name(tables_list):
    '''
    TODO make regex match expected table name
    '''
    tables_list["valid_table_name"] = tables_list.apply(name_validator, axis = 1)
    
    
def type_rules(df):
    if "-values" in df["table_name"]: #or ("Vales" in df["table_name"] and df["schema_name"] == "EXCEED"):
        return "values"
    elif "-description" in df["table_name"]: #or ("variables" in df["table_name"] and df["schema_name"] == "EXCEED"):
        return "descriptions"
    elif len(str(df["field_names"]).split(",")) <= 5:
        return "unclear"
    else:
        return "data"

def get_table_type(df):    
    df["table_type"] = df.apply(type_rules, axis = 1)


    
def get_table_names(cnxn):
    tables_list = pd.read_sql("select schema_name(t.schema_id) as schema_name, "+
                                "t.name as table_name "+
                                "from sys.tables t " +
                                "order by schema_name, table_name ",
                                 cnxn)

    verify_table_name(tables_list)
    return tables_list




def get_row_metadata(cnxn, df):
    '''
    For every table:
        Get number of rows
    '''
    for schema, table in zip(df["schema_name"], df["table_name"]):
        count = cnxn.cursor().execute("select count(*) from ["+schema+"].["+table+"]").fetchone()[0]
        df.loc[df["table_name"] ==table, "row_count"] = count
        


def get_identity(cnx, df):
    '''
    Find and return identity columns
    '''
    pass

    
def get_column_metadata(cnxn, df):
    '''
    for every table:
        get list of field names
        get list of field datatypes
        Make separate dataframe for each table including field and data type
        get fields with id in name
    '''
    tablewise_dfs = {}
    for schema, table in zip(df["schema_name"], df["table_name"]):
        fields = pd.read_sql("SELECT COLUMN_NAME,DATA_TYPE From "+
                            "INFORMATION_SCHEMA.COLUMNS "+
                            "where TABLE_NAME='"+table+"'", cnxn)
        field_names, field_types = ", ".join(fields["COLUMN_NAME"]), ", ".join(fields["DATA_TYPE"])
        tablewise_dfs[schema+"-"+table] = fields

        df.loc[(df["table_name"] == table) & (df["schema_name"] == schema), "field_count"] = len(fields["COLUMN_NAME"])
        df.loc[(df["table_name"] == table) & (df["schema_name"] == schema), "field_names"] = field_names
        df.loc[(df["table_name"] == table) & (df["schema_name"] == schema), 'field_types'] = field_types

    return tablewise_dfs


def get_variable_descriptions(cnxn, df, tablewise_field_data):
    '''
    Assumes done properly (ignore EXCEED which is playing by its own rules)
    description table includes '-description' in name and has fields, 'field', 'meaning',...
    Shares same naming format as data and values table.
    '''
    desc_tables = df.loc[df["table_type"] == "descriptions"]

    for schema, table in zip(desc_tables["schema_name"], desc_tables["table_name"]):
        fields = pd.read_sql("SELECT field as COLUMN_NAME,meaning as MEANING From ["+schema+"].["+table+"]", cnxn)
        tablewise_field_data["DESCRIPTIONS-"+schema+"-"+table] = fields
        
    return tablewise_field_data
            
        


def output(df, tablewise_field_data):
    if not os.path.exists("out"):
        os.mkdir("out")
    df.to_csv(os.path.join("out","all_tables.csv"))
    for key, value in tablewise_field_data.items():
        value.to_csv(os.path.join("out", key+".csv"))

def main(cnxn):
    df = get_table_names(cnxn)
    get_row_metadata(cnxn, df)
    tablewise_field_data = get_column_metadata(cnxn, df)
    get_table_type(df)
    get_variable_descriptions(cnxn, df, tablewise_field_data)
    
    output(df, tablewise_field_data)
    


if __name__ == "__main__":
    cnxn = connect()
    main(cnxn)