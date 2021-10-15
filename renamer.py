import table_checker as tc
import pandas as pd
from datetime import datetime
import re


def renamer(df):
    '''
    Plan:
        1. Split name into '_' separated sections
        2. Identify sections that match/nearly match convention sections
        3. Correct/invent sections
        4. Recombine in correct order
        5. Check if duplicate
    '''
    # Don't include Linkage or NHSD tables
    schema = df["schema_name"]
    if schema == "Linkage" or schema == "HES":
        return df["table_name"]
    elif "nhsd" in df["table_name"]:
        return df["table_name"]

    parts = df["table_name"].split("_")

    # get date (should be consistent because set by swansea)
    date = None
    for part_index in range(len(parts)):
        # If section is a date
        if tc.verify_date_format(parts[part_index]):
            date = parts[part_index]
            remove_index = part_index
    if date:
        del parts[remove_index]
    else:
        date = datetime.today().strftime('%Y%m%d')

    # get version 
    # search for version-like pattern, starting with 4 digits, down to 1 digit
    version_size = None
    for part_index in range(len(parts)):
        for i in range(4,0, -1):
            # attempt to find a "v00..." pattern
            # But! Could be possible for table name to include v0 etc
            # So section must be the same length as pattern (excluding "-desc..." or "-val...")
            pattern = re.compile("^v[0-9]{"+str(i)+"}") 
            if pattern.match(parts[part_index]) and len(parts[part_index].split("-")[0]) == i+1:
                version_size = i
                break

        if version_size:
            version = parts[part_index][:1] + "0"*(4-i) + parts[part_index][1:]
            break
    if not version_size:
        version = "v0001" # Placeholder, not sure how to deal with this
    # Add back in or add "-description" or "-values" where appropriately
    if (("-description" in parts[part_index]) or ("variables" in parts)) and not ("-description" in version):
        version = version + "-description"
    elif (("-values" in parts[part_index]) or ("Vales" in parts)) and not ("-values" in version):
        version = version + "-values"
    if version_size:
        del parts[part_index]

    # If there are other sections to parts that haven't been removed yet
    subblock_number = None
    if len(parts) > 0 and not (len(parts) == 1 and parts[0] == ""):
        if all(map(str.isdigit, parts[-1])):
            subblock_number = parts[-1]
            del parts[-1]
        name = "_".join(parts)
        name = name.replace("-description", "").replace("-values", "")
    else: # The table did not include an identifier
        name = schema.lower() + "_unnamed"
        

    if subblock_number:
        table_name = "_".join([name, version, subblock_number, date])
    else:
        table_name = "_".join([name, version, date])
    # TODO:
    # check for duplicates
    # check for sub block numbers
    # Version number appends too many

    return table_name
    

def main(cnxn):
    df = pd.read_csv("out\\all_tables.csv")[["schema_name","table_name","valid_table_name"]]
    invalid_names = df.loc[df["valid_table_name"] == "No"]
    print(invalid_names)

    invalid_names["suggested_name"] = invalid_names.apply(renamer, axis = 1)
    invalid_names.to_csv("renaming_suggestions.csv")


if __name__ == "__main__":
    cnxn = tc.connect()
    tc.main(cnxn)
    main(cnxn)