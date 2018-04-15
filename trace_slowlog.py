#!/usr/bin/python27
#encoding:utf8
#########################################################################
# ----author:gqhao------------------------------------------------------#
#-----time:2017-09-14---------------------------------------------------#
#-this function is used to collecting slowlogs from the redis servers
#-in product enviroment.and is deployed on SVR4299( server of redis group)
#-Execution frequency :1 time/10 minutes
##########################################################################

import redis
import multiprocessing
import threading
import socket
import sys
from time import sleep
import time
import os
import MySQLdb as my
import datetime
from datetime import datetime
from datetime import timedelta

####检查本地log文件夹是否存在
def check_dir_file():
    basedir="/tmp"
    subdir=""
    dirlist=os.listdir(basedir)
    for item in dirlist:
        if item =="redis_slowlog_check_log":
            subdir=item
            break
    if not subdir:
        try:
            os.mkdir(basedir+"/redis_slowlog_check_log",0754)
        except Exception,e:
            print "make directory ",basedir+"/redis_slowlog_check_log failed,plese make it manually"
            return -1
    return 1
####日志文件维护，删除过期日志，默认保留10天，创建新的日志文件
def maintain_log_file():
    filelist=os.listdir("/tmp/redis_slowlog_check_log")
    currentdate = datetime.now().strftime('%Y%m%d')
    for item in filelist:
        ####删除历史文件
        del_file_date=int(item.replace("redis_slowlog_get_log_",'').replace(".log",''))
        if del_file_date<int((datetime.now()-timedelta(days=5)).strftime('%Y%m%d')):
            try:
                os.remove("/tmp/redis_slowlog_check_log/"+item)
            except Exception,e:
                continue
        ####新增新日志文件
        f = open("/tmp/redis_slowlog_check_log/"+"redis_slowlog_get_log_"+currentdate+".log", "w")
        f.write(currentdate+"\n")
        f.close()

####删除slowlog中因编码原因异常的数据，策略：遇到异常编码字段，删除其和其后的所有数据，会有数据丢失
def data_cleanup(para_string):
    xcode=['6','7','8','9','a','b','c','d','e','f']
    xac_index=para_string.find('\xac')
    if xac_index!=-1:
        para_string=para_string[:xac_index]
    for item_out in xcode:
        for item_in in xcode:
            xe_index=para_string.lower().find(chr(int('0x'+item_out+item_in,16)))
            if xe_index!=-1:
                para_string=para_string[:xe_index]
    return para_string
#### 用于异常数据的批量插入
def insert_data_error(myconn,insert_sql,insert_data):
    cur_myconn=myconn.cursor()
    k=0
#    redis_cluster_name=server[0]
#    redis_host=server[1]
#    redis_port=server[2]
    while True:
        tmp_data1=insert_data[k:k+500]
        tmp_data2=insert_data[k+501:k+502]
        if not tmp_data1:
            break
        if len(tmp_data2) == 1:
            tmp_data1 = insert_data[k:]
            k += 1
        try:
            cur_myconn.executemany(insert_sql,tmp_data1)
            myconn.commit()
        except Exception,e:
            print Exception,e
        del tmp_data1
        del tmp_data2
        k+=500
    del k
    cur_myconn.close()
####slowlogs 写入数据库的主函数，该函数会将收集到的slowlogs写入数据库，redis_monitordb
####有如下特殊情况：
####1.redis slowlog中存储的异常字符无法写入mysql，会被写入到本地文件 redis_slowlog_error_character_type.log
####2.出现slowlog写入数据库失败的情况，该台redis server的slowlog不做reset，以便后续排查

def insert_data_into_mysql(insert_data,insert_sql,collect_instance_name,logfile):
    currentdate = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_flag=1
    insert_result=1
    try:
        myconn=my.connect(host='',port=,db='',user='',passwd='',charset='utf8')
        cur_myconn=myconn.cursor()
    except Exception,e:
        print Exception,e
        sys.exit()
    k=0
    while True:
        tmp_data1=insert_data[k:k+500]
        tmp_data2=insert_data[k+501:k+502]
        if not tmp_data1:
            break
        if len(tmp_data2) == 1:
            tmp_data1 = insert_data[k:]
            k += 1
        try:
            cur_myconn.executemany(insert_sql,tmp_data1)
            myconn.commit()
        except Exception,e:
            insert_flag=0
            insert_result = 0
        if (insert_flag==0) and(collect_instance_name=='slowlog'):
#            tmp_insert_data=[]
            for tmp_item in tmp_data1:
                try:
                    cur_myconn.execute(insert_sql,tmp_item)
                    myconn.commit()
                except Exception,e:
                    print '###################################',currentdate,'###########################################'
                    print 'redis_cluster: ',tmp_item[0]
                    print 'redis_host: ',tmp_item[1]
                    print 'redis_port: ',tmp_item[2]
#                    print 'err_slowlog: ',tmp_item[3]
                    #print Exception,e
#                    tmp_insert_data.append((tmp_item[0],tmp_item[1],tmp_item[2],data_cleanup(tmp_item[3]),tmp_item[4],tmp_item[5]))
#            insert_data_error(myconn,insert_sql,tmp_insert_data)
#            del tmp_insert_data
        insert_flag=1
        del tmp_data1
        del tmp_data2
        k+=500
    del k
    cur_myconn.close()
    myconn.close()
    return insert_result

####slowlogs 采集函数，负责每台服务器上的slowlog 采集
def get_slow_log(sub_serverlist,process_id,logfile):
    try:
        myconn = my.connect(host='', port=, db='', user='',passwd='', charset='utf8')
        cur_myconn = myconn.cursor()
    except Exception, e:
        print Exception, e
        sys.exit()
    currentdate = datetime.now().strftime('%Y%m%d')
    instance_dbsize=[]
    #sql_get_slowlog_lasttime="select slowlog_starttime from redis_slowlog_details_lasttime where redis_host=%s and redis_port=%d"
    sql_insert_slowlog="insert into redis_slowlog_details(cluster_name,redis_host,redis_port,slowlog_command,slowlog_starttime,slowlog_duration) values(%s,%s,%s,%s,%s,%s)"
    sql_insert_slowlog_lasttime="insert into redis_slowlog_details_lasttime(cluster_name,redis_host,redis_port,slowlog_starttime) values(%s,%s,%s,%s)"
    sql_update_slowlog_lasttime = "update redis_slowlog_details_lasttime set slowlog_starttime=%s where redis_host=%s and redis_port =%s"
    sql_insert_dbsize="insert into redis_dbsize_slowlogcount_details(cluster_name,redis_host,redis_port,dbsize,slowlog_count) values(%s,%s,%s,%s,%s)"
    for server in sub_serverlist:
        redis_cluster_name=server[0]
        redis_host=server[1]
        redis_port=server[2]
        firsttime_flag=0
        slowlog_lasttime_tt=[]
        instance_slowlog = []
        print '############################################################################################################################'
        print 'redis_host:',redis_host,'redis_port:',redis_port
        try:
            cur_myconn.execute("select slowlog_starttime from redis_slowlog_details_lasttime where redis_host='"+redis_host+"' and redis_port="+str(redis_port))
            slowlog_lasttime_tt=cur_myconn.fetchone()
        except Exception,e:
            print Exception,e
        if not slowlog_lasttime_tt:
            firsttime_flag=1
        try:
            redis_conn=redis.Redis(host=redis_host,port=int(redis_port),socket_timeout=2)
            dbsize=redis_conn.dbsize()                                         ##get the total key numbers
            #redis_conn.slowlog_reset()
        except Exception,e:
            logfile.write(currentdate+":"+str(process_id)+":"+redis_cluster_name+":"+redis_host+":"+redis_port+'\n')
            dbsize=-1
        slowlog_get_count=1024
        if firsttime_flag==1:
            slowlog_get_count=10240
        try:
#            print 'process_id',process_id,'start slowlog get',datetime.now().strftime('%Y%m%d')
            slowlogs = redis_conn.slowlog_get(slowlog_get_count)
            #redis_conn.slowlog_reset()
        except Exception,e:
            instance_slowlog.append((redis_cluster_name,redis_host,redis_port,'error in connections',currentdate,0))
            slowlogs=[]
            print 'redis_host:',redis_host,'redis_port',redis_port, Exception,e
        #instance_dbsize.append((redis_cluster_name, redis_host, redis_port, dbsize, len(slowlogs)))
        if not slowlogs:
            instance_dbsize.append((redis_cluster_name, redis_host, redis_port, dbsize, len(slowlogs)))
            continue
        if firsttime_flag==1:
            for slowlog in slowlogs:
                if (slowlog['command'].lower().find('slowlog')==-1):
                    instance_slowlog.append((redis_cluster_name,redis_host,redis_port,slowlog['command'],datetime.fromtimestamp(slowlog['start_time']),slowlog['duration']))
        else:
            while True:
                if (datetime.fromtimestamp(slowlogs[0]['start_time'])<=slowlog_lasttime_tt[0]):
                    #slowlogs = []
                    break
                if (datetime.fromtimestamp(slowlogs[-1]['start_time'])<=slowlog_lasttime_tt[0]) or(slowlog_get_count>=10240):
                    for slowlog in slowlogs:
                        if (slowlog['command'].lower().find('slowlog') == -1) and(datetime.fromtimestamp(slowlog['start_time'])>slowlog_lasttime_tt[0]):
                            instance_slowlog.append((redis_cluster_name, redis_host, redis_port, slowlog['command'],
                                                     datetime.fromtimestamp(slowlog['start_time']),
                                                     slowlog['duration']))
                    break
                if (datetime.fromtimestamp(slowlogs[-1]['start_time'])>slowlog_lasttime_tt[0]):
                    slowlog_get_count=slowlog_get_count+2048
                    try:
                        slowlogs = redis_conn.slowlog_get(slowlog_get_count)
                    except Exception,e:
                        print Exception,e
                sleep(1)
        instance_dbsize.append((redis_cluster_name, redis_host, redis_port, dbsize, len(instance_slowlog)))
        insert_data_into_mysql(instance_slowlog, sql_insert_slowlog, 'slowlog', logfile)
        if firsttime_flag==1:
            cur_myconn.execute(sql_insert_slowlog_lasttime,(redis_cluster_name,redis_host,redis_port,datetime.fromtimestamp(slowlogs[0]['start_time'])))
            myconn.commit()
        else:
            cur_myconn.execute(sql_update_slowlog_lasttime,(datetime.fromtimestamp(slowlogs[0]['start_time']),redis_host,redis_port))
            myconn.commit()
        del instance_slowlog
        del slowlogs
    insert_data_into_mysql(instance_dbsize,sql_insert_dbsize,'dbsize_slowlog_count',logfile)
    cur_myconn.close()
    myconn.close()
    logfile.close()
    del instance_dbsize

#### redis server stats 采集程序，for redis 3.0 or above,目前未启用
def get_stats_details(sub_serverlist,process_id,logfile):
    currentdate = datetime.now().strftime('%Y%m%d')
    instance_stats=[]
    sql_insert_stats="insert into redis_stats_details(cluster_name,redis_host,redis_port,total_connections_received,migrate_cached_sockets,total_net_input_bytes,total_net_output_bytes,instantaneous_input_kbps,sync_full,instantaneous_output_kbps,sync_partial_err,instantaneous_ops_per_sec,total_commands_processed,latest_fork_usec,rejected_connections,keyspace_hits,expired_keys,sync_partial_ok) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    for server in sub_serverlist:
        redis_cluster_name=server[0]
        redis_host=server[1]
        redis_port=server[2]
        try:
            redis_conn=redis.Redis(host=redis_host,port=redis_port)
        except Exception,e:
            logfile.write(currentdate+":"+str(process_id)+":"+redis_cluster_name+":"+redis_host+":"+redis_port+":"+e[0])
        info_stats=redis_conn.info('stats')
        instance_stats.append((redis_cluster_name,redis_host,redis_port,info_stats['total_connections_received'],info_stats['migrate_cached_sockets'],info_stats['total_net_input_bytes'],info_stats['total_net_output_bytes'],info_stats['instantaneous_input_kbps'],info_stats['sync_full'],info_stats['instantaneous_output_kbps'],info_stats['sync_partial_err'],info_stats['instantaneous_ops_per_sec'],info_stats['total_commands_processed'],info_stats['latest_fork_usec'],info_stats['rejected_connections'],info_stats['keyspace_hits'],info_stats['expired_keys'],info_stats['sync_partial_ok']))
    insert_data_into_mysql(instance_stats,sql_insert_stats,logfile)
    del instance_stats
#    del redis_conn
####并发开关函数，该函数实现采集程序的并发，以及要并发的程序名称选择
def mutlti_processing_insert(function_name,serverlist,process_num):
    currentdate = datetime.now().strftime('%Y%m%d')
    try:
        logfile = open("/tmp/redis_slowlog_check_log/" + "redis_slowlog_get_log_" + currentdate + ".log", "a")
    except Exception,e:
        print Exception,e
        sys.exit()
    process_list = []
    start_num=0
    end_num=len(serverlist)
    slice_num=(end_num-start_num)/process_num
    if function_name=='get_slow_log':
        for i in range(process_num):
            if i ==process_num-1:
                p=multiprocessing.Process(target=get_slow_log,args=(serverlist[start_num+i*slice_num:],i,logfile))
            else:
                p=multiprocessing.Process(target=get_slow_log,args=(serverlist[start_num+i*slice_num:start_num+(i+1)*slice_num],i,logfile))
            process_list.append(p)
            p.start()
        for process in process_list:
            process.join()
    elif function_name=='get_stats_details':
        for i in range(process_num):
            if i ==process_num-1:
                p=multiprocessing.Process(target=get_stats_details,args=(serverlist[start_num+i*slice_num:],i,logfile))
            else:
                p=multiprocessing.Process(target=get_stats_details,args=(serverlist[start_num+i*slice_num:start_num+(i+1)*slice_num],i,logfile))
            process_list.append(p)
            p.start()
        for process in process_list:
            process.join()
    logfile.close()
###主函数
def main():
    currentdate = datetime.now().strftime('%Y%m%d %H:%M:%S')
    run_log_file_open_flag=1
    try:
        run_log=open('/tmp/redis_slowlog_run_log.log','a')
    except Exception,e:
        print Exception,e
        run_log_file_open_flag=0
    if run_log_file_open_flag==1:
        run_log.write("Redis sloglog getting job starts @"+currentdate+'\n')
    process_num=20
    try:
        myconn=my.connect(host='',port=,db='',user='',passwd='',charset='utf8')
        cur_myconn=myconn.cursor()
    except Exception,e:
        print Exception,e
        sys.exit()
    try:
        cur_myconn.execute("select distinct a.name as cluster_name,b.ip,b.port  from redis_rat.redisadmin_rediscluster a,redis_rat.redisadmin_redisinstance b where a.pool_id=b.pool_id and a.status=1 AND a.sbuid not in (122,188) and b.ip not in ('10.8.87.30','10.8.87.31','0.0.0.0','10.0.0.1','0.0.0.1');")
        serverlist=cur_myconn.fetchall()
    except Exception,e:
        print Exception,e
    mutlti_processing_insert('get_slow_log',serverlist,process_num)
    cur_myconn.close()
    myconn.close()
    ##mutlti_processing_insert('get_stats_details', serverlist, process_num)
    if run_log_file_open_flag==1:
        run_log.write("Redis sloglog getting job has finished @"+currentdate+'\n')
    run_log.close()
	
if __name__=='__main__':
    main()
