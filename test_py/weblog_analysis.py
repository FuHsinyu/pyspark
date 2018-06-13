import re
import datetime
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
import os
from pyspark.sql import SparkSession


def parse_apach_logline(logline):
    APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    if match.group(9) == "-":
        content_size = int(0)
    else:
        content_size = int(match.group(9))
    row_instance = Row(host=match.group(1),
                       client_id=match.group(2),
                       user_id=match.group(3),
                       date_time=match.group(4),
                       method=match.group(5),
                       resource=match.group(6),
                       protocal=match.group(7),
                       response_code=match.group(8),
                       size=content_size,)

    return (row_instance, 1)


logline = ['127.0.0.1 - - [01/Aug/1995:00:00:01 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1839', "123"]

# Get file name
data_saving_dir = "/media/hsin/16FC55FBFC55D619/linux-ubuntu-shared-data"
input_file = os.path.join("access_log_Jul95")
file_name = os.path.join(data_saving_dir, input_file)
sc = SparkContext()
textRDD = sc.textFile(file_name, 4)
# Parse data
parsed_textRDD = textRDD.map(parse_apach_logline)
accessed_log = parsed_textRDD.filter(
    lambda kv: kv[1] == 1).map(lambda kv: kv[0]).cache()
failed_log = parsed_textRDD.filter(
    lambda kv: kv[1] == 0).map(lambda kv: kv[0]).cache()
count_failed_log = failed_log.count()
count_accessed_log = accessed_log.count()
print(failed_log.take(5))
print(accessed_log.count())
# Data Cleaning
pass
# Sample analysis
log_size = accessed_log.map(lambda s: s["size"]).cache()
print(log_size.take(50))
print(log_size.stats())
# Parquet file
# accessed_log.DataFrameWriter.parquet(data_saving_dir+"/df")

