import pyodbc
import getpass
import pandas as pd
import os

def connect():
    '''
    input server connections and login details
    '''
    #server = input("Input server name: ")
    server = "1JZRZ93"
    database = "Test Databases" # temp
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



def verify_table_name(tables_list):
    '''
    TODO make regex match expected table name
    '''
    tables_list["valid_table_name"] = tables_list["table_name"].str.match(r'(^w*)')
    tables_list["valid_table_name"] = "tbc"

def get_table_names(cnxn):
    tables_list = pd.read_sql("select schema_name(t.schema_id) as schema_name, "+
                                "t.name as table_name "+
                                "from sys.tables t; ",
                                 cnxn)

    verify_table_name(tables_list)
    return tables_list

def get_row_metadata(cnxn, df):
    '''
    For every table:
        Get number of rows
    '''
    for schema, table in zip(df["schema_name"], df["table_name"]):
        count = cnxn.cursor().execute("select count(*) from "+schema+"."+table).fetchone()[0]
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
    for  table in df["table_name"]:
        fields = pd.read_sql("SELECT COLUMN_NAME,DATA_TYPE From "+
                            "INFORMATION_SCHEMA.COLUMNS "+
                            "where TABLE_NAME='"+table+"'", cnxn)
        field_names, field_types = ", ".join(fields["COLUMN_NAME"]), ", ".join(fields["DATA_TYPE"])
        tablewise_dfs[table] = fields
        df.loc[df["table_name"] == table, "field_names"] = field_names
        df.loc[df["table_name"] == table, 'field_values'] = field_types
        

    # get field names
    # get field types
    # identify id column
    return tablewise_dfs

def output(df, tablewise_field_data):
    df.to_csv(os.path.join("out","all_tables.csv"))
    for key, value in tablewise_field_data.items():
        value.to_csv(os.path.join("out",key+".csv"))

def main(cnxn):
    df = get_table_names(cnxn)
    get_row_metadata(cnxn, df)
    tablewise_field_data = get_column_metadata(cnxn, df)
    output(df, tablewise_field_data)


if __name__ == "__main__":
    cnxn = connect()
    main(cnxn)