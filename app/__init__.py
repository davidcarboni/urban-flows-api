from flask import Flask, request, jsonify, abort, current_app
from werkzeug.utils import secure_filename
import csv
import re
import os
import json
import tempfile

files = []
app = Flask(__name__)                                                                                                                                                                           

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):

    result = None
    example = 'p_LD0027_24308_20190801T000000_20191031T235959.csv'
    if not path:
        result = files
    else:
        filenames = os.listdir('data/req_20190801T000000_20191031T235959_csv')
        for filename in filenames:
            if len(filename) == len(example) and filename.startswith('p_') and filename.endswith('.csv'):
                if filename.startswith(f'p_{path}'):
                    result = parse(filename)

                    tmax = request.args.get('tmax')
                    if tmax:
                        max=int(tmax)
                        filtered = []
                        for record in result["readings"]:
                            time = int(record["time"])
                            if time <= max:
                                filtered.append(record)
                        result["readings"] = filtered

                    tmin = request.args.get('tmin')
                    if tmin:
                        min=int(tmin)
                        filtered = []
                        for record in result["readings"]:
                            time = int(record["time"])
                            if time >= min:
                                filtered.append(record)
                        result["readings"] = filtered

                    result["metadata"]["count"] = len(result["readings"])
    
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

def nest_metadata(key, value, metadata):
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

def parse(filename):
    """ Parses a CSV file """
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

    with open(f"data/req_20190801T000000_20191031T235959_csv/{filename}", 'r') as csvfile:
        line = csvfile.readline()
        while line:

            if line.startswith('#'):

                # Parse header lines
                print(f"Header: {line}")

                if line.startswith('# The Urban Flows Observatory Sheffield'):
                    metadata["info"].append(line[2:].strip())
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
                                nest_metadata(key, value, metadata)
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
    
    return records

def range(records):
    """ Computes that tmin and tmax of all record times. """
    readings = records["readings"]
    max = int(readings[0]["time"])
    min = int(readings[0]["time"])
    for reading in readings:
        time = int(reading["time"])
        if time < min:
            min = time
        if time > max:
            max = time
    return {"tmin": min, "tmax": max}


# Parse and cache file info, because this takes a while to compute

example = 'p_LD0027_24308_20190801T000000_20191031T235959.csv'
filenames = os.listdir('data/req_20190801T000000_20191031T235959_csv')
for filename in filenames:
    if len(filename) == len(example) and filename.startswith('p_') and filename.endswith('.csv'):
        segments = filename.split('_')
        records = parse(filename)
        site = records["metadata"]["site"]["id"]
        sensor = records["metadata"]["sensor"]["id"]
        files.append({
            "link": f'{segments[1]}_{segments[2]}?tmax=&tmin=',
            "site": site,
            "sensor": sensor,
            "range": range(records)
        })

if __name__ == '__main__':
    #app.run(host='0.0.0.0')
    pass