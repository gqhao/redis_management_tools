#!/usr/bin/python27
#encoding:utf8
import paramiko
import redis
import socket
import sys
from time import sleep
import time
import MySQLdb as my
import pymssql as py
user_serveradmin=''
passwd_serveradmin=''

#######获取当前时间
def getdate():
  return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

#######创建数据库连接
def get_database_connections(host,port,db,user,password,dbtype):
    if dbtype.lower()=='mysql':
        try:
            myconn=my.connect(host=host,port=port,db=db,user=user,passwd=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1,''
        return 1,myconn
    elif dbtype.lower()=='sqlserver':
        try:
            mssqlconn=py.connect(host=host,port=port,database=db,user=user,password=password,charset='utf8')
        except Exception,e:
            print Exception,e
            return -1, ''
        return 1,mssqlconn
    else:
        return -1,''
#####安全删除sentinel
def delete_sentinel(group_name,host,port):
  myconn_info_flag, myconn_info = get_database_connections(host='',port=, db='',user=user_serveradmin,password=passwd_serveradmin, dbtype='mysql')
  if myconn_info_flag == -1:
    print getdate(), 'Main function: connects datatransfer_control fails'
    return((-1,'Connect redis-rat error,please check manully!'))
  cur_myconn_info=myconn_info.cursor()
  try_count=0
  while True:
    if try_count>5:
      break
    tt_sentinel_list = []
    try:
      reconn=redis.Redis(host=host,port=port)
      tt_sentinel_list=reconn.client_list()
    except Exception,e:
      print Exception,e
      return ((-1,'Connect redis server '+host+':'+str(port)+' error,please check manully!'))
    if not tt_sentinel_list:
      return((-1,'No connection found'))
    sentinel_list=[]
    for item in tt_sentinel_list:
      if item['name'].find('sentinel')!=-1:
        tt_addr=item['addr'].split(':')
        if tt_addr[0]:
          sentinel_list.append(tt_addr[0])
    if not sentinel_list:
      sleep(1)
      try_count+=1
      continue
    sentinel_in_condition=""
    for item in sentinel_list:
      sentinel_in_condition+="'"+item+"',"
    sentinel_in_condition=sentinel_in_condition[:-1]
    print "select distinct group_id from redis_sentinel_list_new where ip in ( "+sentinel_in_condition+")"
    cur_myconn_info.execute("select distinct group_id from redis_sentinel_list_new where ip in ( "+sentinel_in_condition+")")
    tt_group=cur_myconn_info.fetchall()
    for item in tt_group:
      cur_myconn_info.execute("select distinct ip,port from redis_sentinel_list_new where group_id="+str(item[0]))
      tt_sentinel_list=cur_myconn_info.fetchall()
      for tt_sentinel in tt_sentinel_list:
        print tt_sentinel
        try:
          sentinel_conn=redis.Redis(host=tt_sentinel[0],port=tt_sentinel[1])
          sentinel_conn.sentinel_remove(group_name)
        except Exception,e:
          print Exception,e
          print '#################################'
          print tt_sentinel
          print '#################################'
          #return ((-1,'sentinel '+tt_sentinel[0]+':'+str(tt_sentinel[1])+' remove '+group_name+'error,please retry again'))
    try_count+=1
  cur_myconn_info.close()
  myconn_info.close()
  return ((1,'Remove '+group_name+'from sentinels successfully!'))








