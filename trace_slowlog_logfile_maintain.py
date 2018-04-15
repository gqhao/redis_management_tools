#encoding:utf-8
import sys
import os
import time
import datetime
import MySQLdb as my
from datetime import timedelta
from datetime import datetime
########返回当前时间
def getdate():
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def maintain_slowlog_errorlog_file():
  currentdate = datetime.now().strftime('%Y%m%d%H%m%S')
  del_file_date=(datetime.now()-timedelta(days=4)).strftime('%Y%m%d')
  has_logfile=0
  has_logdir=0

  tt_filelist=os.listdir("/tmp")
  for item in tt_filelist:
    if item =='redis_slowlog_running_status_sys.log':
      has_logfile=1
      break
  for item in tt_filelist:
    if item =="redis_slowlog_check_log":
      has_logdir=1
      break
  if has_logdir==0:
    try:
      os.mkdir("/tmp/redis_slowlog_check_log",0754)
    except Exception,e:
      print Exception,e
      print "Creaet directory error,please make it manually"
  else:
    del_filelist=os.listdir("/tmp/redis_slowlog_check_log")
    for item in del_filelist:
      if item.find("redis_slowlog_running_status_sys_"+del_file_date)!=-1:
        os.system("rm -rf /tmp/redis_slowlog_check_log/"+item)
        print "rm -rf /tmp/redis_slowlog_check_log/"+item

  if has_logfile==1:
    os.system("mv /tmp/redis_slowlog_running_status_sys.log /tmp/redis_slowlog_check_log/redis_slowlog_running_status_sys_"+currentdate+".log")

def maintain_partition_table():
  try:
    myconn = my.connect(host='', port=, db='', user='',
                        passwd='', charset='utf8')
    cur_myconn = myconn.cursor()
  except Exception, e:
    print Exception, e
    sys.exit()
  try:
    cur_myconn.execute("call configdb.sp_partition_maintain('redis_monitordb','redis_slowlog_details','D','5',15,'D');")
    cur_myconn.nextset()
  except Exception,e:
    print Exception,e
  try:
    cur_myconn.execute("call configdb.sp_partition_maintain('redis_monitordb','redis_dbsize_slowlogcount_details','D',5,15,'D');")
    cur_myconn.nextset()
  except Exception,e:
    print Exception,e
  try:
    cur_myconn.execute("call configdb.sp_partition_maintain('redis_rat','redis_group_check_result','D',5,15,'D');")
    cur_myconn.nextset()
  except Exception,e:
    print Exception,e
  try:
    cur_myconn.execute("call configdb.sp_partition_maintain('redis_rat','redis_group_check_result_ops','D',5,15,'D');")
    cur_myconn.nextset()
  except Exception,e:
    print Exception,e
def main():
  print "slowlog_running_sys_maintain\n"
  maintain_slowlog_errorlog_file()
  print "partition table maintain\n"
  maintain_partition_table()

if __name__=='__main__':
  main()
