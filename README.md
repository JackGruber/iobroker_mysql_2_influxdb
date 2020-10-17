# Migrate ioBroker history datapoints from MySQL to influxDB

Python script to migrate ioBroker MySQL (SQL History Adapter) data to influxDB (Logging data with influxDB)

Based on a script from UlliJ from the [ioBroker Forum](https://forum.iobroker.net/topic/12482/frage-migrate-mysql-nach-influxdb/26)

I have used the script to migrate 22 million records from a MySQL DB running on a Rasperry Pi to an influxDB on a Synology.

## Infos

- Run `pip install -r requirements.txt` to install all requirements
- The script processes all measuring points from `ts_number`, `ts_bool` and `ts_string`
- The data is retrieved in a batch of 100,000 data sets per measuring point and sent to influxDB in batches of 1,000 and the progress is displayed.
- Rename `database.json.example` to `database.json` and adjust the values for `host`, `port`, `user`, `passwd` and `db` to your enviroment
