import os
import time
from boto.emr.connection import EmrConnection
from boto.s3.connection import S3Connection
from boto.emr.step import StreamingStep
from boto.s3.key import Key

#add your amazon creds to bash environment following the below
s3_bkt = os.environ['S3_BKT']
aws_access_key = os.environ['AWSAccessKeyId']
aws_secret_key = os.environ['AWSSecretKeyId']

def status_check(emr_conn, jobid):
  status = 0
  while status not in ['COMPLETED', 'FAILED', 'TERMINATED' ]:
    status = emr_conn.describe_jobflow(jobid).state
    print 'running %s: state is %s' % (jobid, status)
    time.sleep(30)


if __name__ == '__main__':
  #connect to s3 and emr
  emr_conn = EmrConnection(aws_access_key, aws_secret_key)
  s3_conn = S3Connection(aws_access_key, aws_secret_key)

  #upload mapper
  bucket = s3_conn.create_bucket(s3_bkt)
  k = Key(bucket)
  k.key = 'mapper.py'
  k.set_contents_from_filename('mapper.py')

  #where data comes from
  mapper_uri = 's3n://%s/mapper.py' % (s3_bkt)
  output_uri = 's3n://%s/output' % (s3_bkt)
  log_uri = 's3n://%s/log' % (s3_bkt)

  #configure the step
  wc_step = StreamingStep(name='My Hello World Count',
    mapper=mapper_uri,
    reducer='aggregate',
    input='s3n://elasticmapreduce/samples/wordcount/input',
    output=output_uri)
  
  #launch job
  jobid = emr_conn.run_jobflow(name='My hello word count job',
    log_uri=log_uri,
    steps=[wc_step] )
  
  #status check and exit
  status_check(emr_conn, jobid)

