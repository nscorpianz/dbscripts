##############################################################################
## custom python script to sync/transfer data between 2 different mysql server 
##
##############################################################################
import pandas as pd
import optparse
import mysql.connector
from sqlalchemy import create_engine
import sys
import timeit

import warnings
warnings.filterwarnings('ignore')

parser = optparse.OptionParser()

parser.add_option('--src_dbhost', action="store", dest="src_host",  help="source db hostname/IP/endpoint", default="")
parser.add_option('--src_dbname', action="store", dest="src_dbname",  help="source db name", default="")
parser.add_option('--src-table', action="store", dest="src_table",  help="source db table name", default="")
parser.add_option('--src-port', action="store", dest="src_port",  help="source db server port", default=3306)
parser.add_option('--src-username', action="store", dest="src_username",  help="source db username", default="")
parser.add_option('--src-passwd', action="store", dest="src_password",  help="source db user passwd", default="")

parser.add_option('--dest_dbhost', action="store", dest="dest_host",  help="destination db hostname/IP/endpoint", default="")
parser.add_option('--dest_dbname', action="store", dest="dest_dbname",  help="destination db name", default="")
parser.add_option('--dest_table', action="store", dest="dest_table",  help="destination db table name", default="")
parser.add_option('--dest-port', action="store", dest="dest_port",  help="destination db server port", default=3306)
parser.add_option('--dest-username', action="store", dest="dest_username",  help="destination db username", default="")
parser.add_option('--dest-passwd', action="store", dest="dest_password",  help="destination db user passwd", default="")

parser.add_option('--where', action="store", dest="where_cond",  help="where condition for table query", default="")
parser.add_option('--chunk-size', action="store", dest="chunk_size",  help="number of records to be dumped at once in case of huge table", default=1)
parser.add_option('--txn-size', action="store", dest="txn_size",  help="Total number of rows that needs to be dumped", default=1)
parser.add_option('--dry-run', action="store", dest="dry_run",  help="only query to be printed (True|False)", default='False')

options, args = parser.parse_args()

print("options : ",options)
print(" args : ", args)

############ definitions

def create_db_conn(config):
  return mysql.connector.connect(host=config['host'], port=config['port'], database=config['database'], user=config['user'], passwd=config['passwd'])


def dest_db_conn(config):
    return create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(config['user'], config['passwd'] , config['host'], config['database']))


############ src db config
src_config = {'host':options.src_host, 'port':options.src_port, 'user':options.src_username, 'passwd':options.src_password, 'database':options.src_dbname, 'table': options.src_table}

############ dest db config
dest_config = {'host':options.dest_host, 'port':options.dest_port, 'user': options.dest_username, 'passwd':options.dest_password, 'database':options.dest_dbname, 'table': options.dest_table}

#print(','.join(["{}='{}'".format(x,y) for x,y in src_config.items() if x!='table' ]))

qry = "SELECT * FROM {table}".format(table=options.src_table)


if len(options.where_cond) < 3:
    exit("missing where condition")
else:
    qry += " WHERE {}".format(options.where_cond)

txn_size = 0 if len(str(options.txn_size).strip()) < 1 else int(options.txn_size)
chunk_size = 0 if len(str(options.chunk_size).strip()) < 1 else int(options.chunk_size)
dry_run = False if len(str(options.dry_run).strip()) < 1 else options.dry_run.strip().lower().capitalize()

print("txn_size : ",txn_size)
print("dry_run : ", dry_run)

if (txn_size) < 1:
   if dry_run == 'True':
       txn_size=100
   else:
       pass

print("txn_size : ",txn_size)


loop_cnt = int(txn_size/chunk_size)
loop_cnt = loop_cnt if (loop_cnt * chunk_size) == txn_size else loop_cnt + 1

limit_range=""

try:
    src_conn=create_db_conn(src_config)
except Exception as e:
    exit("exception while connecting source db : "+str(e))

try:
    dest_conn=dest_db_conn(dest_config)
except Exception as e:
    exit("exception while connection destination db : "+str(e))

for i in range(0,loop_cnt):
  if i == 0:
    limit_range = "LIMIT {}, {}".format(i, chunk_size)
  else:
    limit_range = "LIMIT {}, {}".format((chunk_size * i) + 1, chunk_size)

  source_query = qry + " " + limit_range
  print(source_query)
  if dry_run == 'False':
      print("inside if")
      source_df = pd.read_sql(source_query, src_conn)
      print("total record : ",source_df.shape)
      source_df.to_sql(name=options.dest_table, con=dest_conn, if_exists='append', index=False)

src_conn.close()
dest_conn.close()
