import json
import os
import sys
import time
from influxdb import InfluxDBClient
import pymysql

# Load DB Settings
database_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "database.json")
if not os.path.exists(database_file):
    print("Please rename database.json.example to database.json")
    sys.exit(1)

f = open(database_file, 'r')
db = f.read()
f.close()
db = json.loads(db)

try:
    MYSQL_CONNECTION = pymysql.connect(host = db['MySQL']['host'],
                                    port = db['MySQL']['port'],
                                    user = db['MySQL']['user'],
                                    password = db['MySQL']['password'],
                                    db = db['MySQL']['database'])
except pymysql.OperationalError as error:
    print (error)
    sys.exit(1)
except Exception as ex:
    print("MySQL connection error")
    print(ex)
    sys.exit(1)


INFLUXDB_CONNECTION = InfluxDBClient(host = db['InfluxDB']['host'],
                                     port = db['InfluxDB']['port'],
                                     username = db['InfluxDB']['user'],
                                     password = db['InfluxDB']['password'],
                                     database = db['InfluxDB']['database'])

# Select datapoints
if len(sys.argv) > 1 and sys.argv[1].upper == "ALL":
    MIGRATE_DATAPOINT = ""
    print("Migrate ALL datapoints ...")
elif len(sys.argv) == 2:
    MIGRATE_DATAPOINT = " AND name LIKE '" + sys.argv[1] + "' "
    print("Migrate '" + sys.argv[1] + "' datapoint(s) ...")
else:
    print("To migrate all datapoints run '" + sys.argv[0] + " ALL'")
    print("To migrate one datapoints run '" + sys.argv[0] + " <DATAPONTNAME>'")
    print("To migrate a set of datapoints run '" + sys.argv[0] + ' "hm-rega.0.%"' + "'")
    sys.exit(1)
print("")

# dictates how columns will be mapped to key/fields in InfluxDB
SCHEMA = {
    "time_column": "time", # the column that will be used as the time stamp in influx
    "columns_to_fields" : ["ack","q", "from","value"], # columns that will map to fields 
    # "columns_to_tags" : ["",...], # columns that will map to tags
    "table_name_to_measurement" : "name", # table name that will be mapped to measurement
    }


#####
#Generates an collection of influxdb points from the given SQL records
#####
def generate_influx_points(records):
    influx_points = []
    for record in records:
        #tags = {}, 
        fields = {}
        #for tag_label in SCHEMA['columns_to_tags']:
        #   tags[tag_label] = record[tag_label]
        for field_label in SCHEMA['columns_to_fields']:
            fields[field_label] = record[field_label]
        influx_points.append({
            "measurement": record[SCHEMA['table_name_to_measurement']],
            #"tags": tags,
            "time": record[SCHEMA['time_column']],
            "fields": fields
        })

    return influx_points

def query_metrics(table):
    MYSQL_CURSOR.execute("SELECT name, id FROM datapoints WHERE id IN(SELECT DISTINCT id FROM " + table + ")" + MIGRATE_DATAPOINT)
    rows = MYSQL_CURSOR.fetchall()
    print('Total metrics in ' + table + ": " + str(MYSQL_CURSOR.rowcount))
    return rows

def migrate_datapoints(table):
    query_max_rows = 100000 # prevent run out of mermory limit on SQL DB
    process_max_rows = 1000     
    
    migrated_datapoints = 0    
    metrics = query_metrics(table)
    metric_nr = 0
    metric_count = str(len(metrics))
    processed_rows = 0
    for metric in metrics:
        metric_nr += 1
        print(metric['name'] + " (" + str(metric_nr) + "/" + str(metric_count) + ")")
        
        start_row = 0
        processed_rows = 0
        while True:
            query  = """SELECT d.name,
                                m.ack AS 'ack',
                                (m.q*1.0) AS 'q',
                                s.name AS "from",
                                (m.val*1.0) AS 'value',
                                (m.ts*1000000) AS'time'
                                FROM """ + table + """ AS m
                                LEFT JOIN datapoints AS d ON m.id=d.id
                                LEFT JOIN sources AS s ON m._from=s.id
                                WHERE q=0 AND d.id = """ + str(metric['id']) + """
                                ORDER BY m.ts desc
                                LIMIT """ + str(start_row)+ """, """ + str(query_max_rows)
            MYSQL_CURSOR.execute(query)
            if MYSQL_CURSOR.rowcount == 0:
                break
            
            # process x records at a time
            while True:            
                selected_rows = MYSQL_CURSOR.fetchmany(process_max_rows)
                if len(selected_rows) == 0:
                    break
                
                print(f"Processing row {processed_rows + 1:,} to {processed_rows + len(selected_rows):,} from LIMIT {start_row:,} / {start_row + query_max_rows:,} " + table + " - " + metric['name'] + " (" + str(metric_nr) + "/" + str(metric_count) + ")")
                migrated_datapoints += len(selected_rows)

                try:
                    INFLUXDB_CONNECTION.write_points(generate_influx_points(selected_rows))
                except Exception as ex:
                    print("InfluxDB error")
                    print(ex)
                    sys.exit(1)               
                
                processed_rows += len(selected_rows)
                
            start_row += query_max_rows
        print("")
        
    return migrated_datapoints

MYSQL_CURSOR = MYSQL_CONNECTION.cursor(cursor=pymysql.cursors.DictCursor) 
migrated = 0
migrated += migrate_datapoints("ts_number")
migrated += migrate_datapoints("ts_bool")
migrated += migrate_datapoints("ts_string")
print(f"Migrated: {migrated:,}")


MYSQL_CONNECTION.close()