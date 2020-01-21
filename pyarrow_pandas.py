import pyarrow.parquet as pq
import s3fs
import pandas as pd
import datetime
import os
import psutil
pd.options.display.html.table_schema = True

s3 = s3fs.S3FileSystem()


process = psutil.Process(os.getpid())
data_dir='s3://edo-ipm2-prod-aa/s2/data/rdb/dl_access/subscriber/p_date=2019-09-01/'


def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.memory_info()[0] / float(2 ** 20)
    return mem
  
files = ["s3://"+f for f in s3.ls(data_dir) if 'part' in f]

def add_sizes(files):
  return sum([s3.du(f) for f in files])/ float(2 ** 20)

attributes=[]
for n_files in [1,2,4,8]:
  total_filesize_mb = add_sizes(files[:n_files])
  t0 = datetime.datetime.now()
  pandas_dataframe = pq.ParquetDataset(files[:n_files], filesystem=s3).read_pandas().to_pandas()
  t1 = datetime.datetime.now()
  mem = memory_usage_psutil()
  
  record_cnt=pandas_dataframe.shape[0]
  details = (n_files,total_filesize_mb,record_cnt,mem,(t1-t0).total_seconds())
  print(details)
  attributes.append(details)
  del(pandas_dataframe)


# Only 2.8GB worth of Parquet data loadable to 80GB session
# At 8GB, it fails
