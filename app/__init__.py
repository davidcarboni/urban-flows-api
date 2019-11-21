from flask import Flask, jsonify, abort, current_app
from werkzeug.utils import secure_filename
import csv
import re
import os
import json
import tempfile

records = {
    "metadata" : {
        "info": [],
        "column_descriptions": [],
        },
    "columns": [],
    "readings": [],
}
metadata = records["metadata"]
columns = records["columns"]
readings = records["readings"]
rows = []
app = Flask(__name__)                                                                                                                                                                           

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    global records

    result = None
    if not path:
        # Return all codes
        result = records
    else:
        # Return a requested code
        for reading in records["readings"]:
            if path.lower() == reading.get("time").lower():
                result = reading
    
    if not result:
        # Not found
        abort(404)

    return jsonify(result)


# @app.before_first_request
# def parse_csv():
#     """ Parse the CSV of data """

def parse_descriptions(value):
    descriptions = value.split('/')
    for idx, val in enumerate(descriptions):
        descriptions[idx] = val.strip()
    return descriptions

def describe_column(fields, values):
    result = {}
    for idx, val in enumerate(fields):
        result[val] = values[idx]
    return result

def nest_metadata(key, value):
    path = key.split('.')
    print(path)
    pos = 0
    leaf = len(path) - 1
    current = metadata
    while pos < leaf:
        if not current.get(path[pos]):
            metadata[path[pos]] = {}
        current = current[path[pos]]
        pos += 1
    current[path[pos]] = value
        

with open("data/req_20190801T000000_20191031T235959_csv/p_LD0057_24319_20190801T000000_20191031T235959.csv", 'r') as csvfile:
    line = csvfile.readline()
    while line:

        if line.startswith('#'):

            # Parse header lines
            print(f"Header: {line}")

            if line.startswith('# Begin CSV table for '):
                metadata["info"].append(line.replace('# Begin CSV table for ', '').strip())
            else:
                # "# key: value"
                match = re.search('#\\s([^\\s^:]+):\\s*(.*)', line)
                if match:
                    key = match.group(1)
                    value = match.group(2)
                    if key == 'ColDescription':
                        column_fields = parse_descriptions(value)
                        print(f"Column description fields: {column_fields}")
                    else:
                        path = key.split('.')
                        if len(path) == 1:
                            metadata[key] = value
                        else:
                            nest_metadata(key, value)
                        print(f'Metadata: {key} : {value}')

                # "# Column_1 / ... / ..."
                match = re.search('#\\sColumn_(\\d)\\s/\\s(.*)', line)
                if match:
                    column_number = match.group(1)
                    column_description = parse_descriptions(match.group(2))
                    column = describe_column(column_fields, column_description)
                    metadata["column_descriptions"].append(column)
                    # We'll use this as headers for parsing the data rows:
                    columns.append(column["name"])
                    print(f"Column {column_number}: {column}")

        else:

            # Collect data rows
            rows.append(line)

        line = csvfile.readline()

    print(f"Metadata: {metadata}")
    print(f"Columns: {columns}")
    print(f"Number of rows: {len(rows)}")

# Write out a clean csv:

with tempfile.NamedTemporaryFile(delete=False) as temp:
    tempfilename = temp.name

with open(tempfilename, 'w') as csvfile:
    fieldnames = ['first_name', 'last_name']
    writer = csv.DictWriter(csvfile, fieldnames=columns)
    writer.writeheader()

with open(tempfilename, 'a') as csvfile:
    csvfile.writelines(rows)
rows = None

# Read the clean csv:

with open(tempfilename) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        #if len(readings)<100:
        readings.append(row)

print("Parsed.")

if __name__ == '__main__':
    #app.run(host='0.0.0.0')
    pass