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
import getopt

user_serveradmin=''
passwd_serveradmin=''

########返回当前时间
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

##check the whether the server is available
def checkopen(dns, port):
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.settimeout(2)
    check_times=5
    check_count=0
    check_result=1
    while True:
        if check_count>=check_times:
            break
        try:
            sk.connect((dns, int(port)))
        except:
            check_result=0
            sleep(1)
        check_count+=1
        if check_result==1:
            break
    sk.close()
    return check_result

#####get the slave(s) node information (ip address )
def get_slave_nodes(keyslist):
    slave_list=[]
    keys=keyslist.keys()
    for item in keys:
        if item.find('slave')==0:
            slave_list.append(keyslist[item])
    return slave_list
###get the human-friendly key-size
def get_Human_Size(value):
    gb = 1024 * 1024 * 1024.0
    mb = 1024 * 1024.0
    kb = 1024.0
    if value >= gb:
        return str(round(value / gb, 2)) + " gb"
    elif value >= mb:
        return str(round(value / mb, 2)) + " mb"
    elif value >= kb:
        return str(round(value / kb, 2)) + " kb"
    else:
        return str(value) + "b"

######get key list
def get_key_list(reconn):
    keyslist=[]
    tmp_keyslist=reconn.scan(0)
    for item in tmp_keyslist[1]:
        keyslist.append(item)
    while True:
        if tmp_keyslist[0]==0:
            break
        tmp_keyslist=reconn.scan(tmp_keyslist[0])
        for item in tmp_keyslist[1]:
            keyslist.append(item)
    return keyslist
######## get zset member#########
def get_zset_member(reconn,zset_keyname):
    member_list=[]
    tmp_member_list=reconn.zscan(zset_keyname,0)
    for item in tmp_member_list[1]:
        member_list.append(item[0])
    while True:
        if tmp_member_list[0]==0:
            break
        tmp_member_list=reconn.zscan(zset_keyname,tmp_member_list[0])
        sleep(0.005)
        for item in tmp_member_list[1]:
            member_list.append(item[0])
    return member_list
def get_nottl_keys(reconn,keyslist):
    redis_pipe=reconn.pipeline()
    commit_flag=0
    check_result=[]
    nottl_keyslist=[]
    for item in keyslist:
        if commit_flag==200:
            commit_flag=0
            tt_result=redis_pipe.execute()
            for tt_item in tt_result:
                check_result.append(tt_item)
        redis_pipe.ttl(item)
        commit_flag+=1
    tt_result = redis_pipe.execute()
    for tt_item in tt_result:
        check_result.append(tt_item)
    for i in range(len(check_result)):
        if not check_result[i]:
            nottl_keyslist.append(keyslist[i])
    return nottl_keyslist

###### get hash fields################
def get_hash_field(reconn,hash_keyname):
    field_list = []
    tmp_field_list = reconn.hscan(hash_keyname, 0)
    for item in tmp_field_list[1].keys():
        field_list.append(item)
    while True:
        if tmp_field_list[0] == 0:
            break
        tmp_field_list = reconn.hscan(hash_keyname, tmp_field_list[0])
        sleep(0.005)
        for item in tmp_field_list[1].keys():
            field_list.append(item)
    return field_list
########get the os available memory
def get_availabe_memroy_os(host):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, 22, username='admin', password='cEwxqv1U4uh3c;Xkvr;n')
        stdin, stdout, stderr = ssh.exec_command("cat /proc/meminfo |grep MemFree")
        output = stdout.readline()
        memory_free = int(output.replace('\n', '').replace('MemFree', '').replace(' ', '').replace(':', '').replace('kB', ''))
        ssh.close()
        if not output:
            return -1
        return memory_free
    except Exception as e:
        return -1

########get the os available memory
def get_availabe_memroy_os_debug(host):
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host, 1022, username='op1',timeout=1, password='vulwaTY7mgrwree+3dsw')
        stdin, stdout, stderr = ssh.exec_command("cat /proc/meminfo |grep MemFree")
        output = stdout.readline()
        memory_free = int(
            output.replace('\n', '').replace('MemFree', '').replace(' ', '').replace(':', '').replace('kB', ''))
        ssh.close()
        if not output:
           return -1
        return memory_free
    except Exception as e:
        return -1

######get key type
def get_key_type(reconn,keyname):
    return reconn.type(keyname)

def delete_key(myconn,reconn_slave,reconn,keyname,task_id,createuser):
    try:
        reconn_local=redis.Redis(host='127.0.0.1',port=6379)
        reconn_local.hset('safely_delete_key', 'emergency_stop',0)
    except Exception,e:
        print Exception,e
    cur_myconn=myconn.cursor()
    finished_key_number=0
    redis_pipe=reconn.pipeline()
    key_type=get_key_type(reconn,keyname)
    if key_type=='string':                       ######delete keys which the key type is string
        #print '########', keyname, ' is a string key,I will call delete to delete the key if the key size is lower than 1GB'
        key_size=reconn.strlen(keyname)
        if key_size>10240000:
            cur_myconn.close()
            print 'The string key is bigger than 100 MB,please delete it manually'
            return (-1,'The string key is bigger than 100 MB,please delete it manually')
        try:
            reconn.delete(keyname)
        except Exception,e:
            cur_myconn.close()
            print Exception,e
            return (-1,'Something wrong has happend,please check manually!')
        cur_myconn.close()
        return(1,'The key '+keyname+' has been deleted successfully!')
    elif key_type=='list':                        ######delete keys which the key type is list
        #print '########', keyname, ' is a list key,I will call lpop to delete content,then deleting the key'
        commit_flag=0
        write_log_flag=0
        key_len=reconn.llen(keyname)
        while True:
            if write_log_flag>=10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                try:
                    cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number)+'/'+str(key_len) , createuser))
                    myconn.commit()
                except Exception,e:
                    print Exception,e
            current_llen=reconn.llen(keyname)
            if not current_llen:
                try:
                    reconn.delete(keyname)
                except Exception,e:
                    cur_myconn.close()
                    print Exception,e
                    return (-1, 'Something wrong has happend,please check manually!')
                cur_myconn.close()
                return (1,'The key '+keyname+' has been deleted successfully!')
            if commit_flag==200:
                print 'keylist_length:',reconn.llen(keyname)
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    cur_myconn.close()
                    return (-1, 'Emergency stop,please check manually!')
                tt=redis_pipe.execute()
                del tt
                commit_flag=0
                sleep(0.1)
            if current_llen<200:
                #print 'keylist_length:', reconn.llen(keyname)
                tt=redis_pipe.execute()
                del tt
            redis_pipe.lpop(keyname)
            commit_flag+=1
            finished_key_number +=1
            write_log_flag +=1
    elif key_type=='set':                         ######delete keys which the key type is set
        #print '########', keyname, ' is a set key,I will call spop to delete members,then deleting the key'
        commit_flag=0
        write_log_flag=0
        key_len=reconn.scard(keyname)
        while True:
            if write_log_flag>=10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                try:
                   cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number)+'/'+str(key_len) , createuser))
                   myconn.commit()
                except Exception,e:
                    print Exception,e
            current_llen = reconn.scard(keyname)
            if not current_llen:
                try:
                    reconn.delete(keyname)
                except Exception,e:
                    cur_myconn.close()
                    print Exception,e
                    return (-1, 'Something wrong has happend,please check manually!')
                cur_myconn.close()
                return (1, 'The key ' + keyname + ' has been deleted successfully!')
            if commit_flag==200:
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    cur_myconn.close()
                    return (-1, 'Emergency stop,please check manually!')
                #print 'set commit'
                redis_pipe.execute()
                commit_flag=0
                sleep(0.1)
            if current_llen<200:
                redis_pipe.spop(keyname)
            redis_pipe.spop(keyname)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
    elif key_type=='zset':                         ######delete keys which the key type is zset
        #print '########', keyname, ' is a zset key,I will call zrem to delete fileds,then deleting the key'
        commit_flag=0
        write_log_flag=0
        print '######## Getting zset members!'
        cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",
            (task_id, u'任务执行中', 'info', u'获取zset类型key的members', 'gqhao'))
        myconn.commit()
        member_list=get_zset_member(reconn_slave,keyname)
        try:
            cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', u'members 已获取，开始删除',createuser))
            myconn.commit()
        except Exception,e:
            print Exception,e
        #print '######## zset members getted! Now start zset key deleting!'
        key_len=len(member_list)
        for item in member_list:
            if write_log_flag>=10000:
                write_log_flag=0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                try:
                    cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number) +'/'+str(key_len) , createuser))
                    myconn.commit()
                except Exception,e:
                    print Exception,e
            if commit_flag==200:
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    return (-1, 'Emergency stop,please check manually!')
                redis_pipe.execute()
                commit_flag=0
                sleep(0.1)
            redis_pipe.zrem(keyname,item)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
        redis_pipe.execute()
        del member_list
        if finished_key_number > key_len:
            finished_key_number = key_len
            try:
                cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number) + '/' + str(key_len), createuser))
                myconn.commit()
            except Exception,e:
                print Exception,e
        try:
            reconn.delete(keyname)
        except Exception,e:
            cur_myconn.close()
            print Exception,e
            return (-1, 'Something wrong has happend,please check manually!')
        cur_myconn.close()
        return (1, 'The key ' + keyname + ' has been deleted successfully!')
    elif key_type=='hash':                         ######delete keys which the key type is hash
        print '########',keyname,' is a hash key,I will call hdel to delete fileds,then deleting the key'
        commit_flag=0
        write_log_flag=0
        finished_key_number=0
        #print '######## Getting hash fileds!'
        try:
            cur_myconn.execute( "insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)", (task_id, u'任务执行中', 'info', u'获取hash类型key的fields',createuser))
            myconn.commit()
        except Exception,e:
            print Exception,e
        field_list=get_hash_field(reconn_slave,keyname)
        try:
            cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', u'fields 已获取，开始删除',createuser))
            myconn.commit()
        except Exception,e:
            print Exception,e
        #print '######## hash fields getted! Now start hash key deleting!'
        key_len=len(field_list)
        for item in field_list:
            if write_log_flag>=10000:
                write_log_flag = 0
                if finished_key_number>key_len:
                    finished_key_number=key_len
                try:
                    cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number)+'/' +str(key_len), createuser))
                    myconn.commit()
                except Exception,e:
                    print Exception,e
            if commit_flag==200:
                sleep(0.1)
                try:
                    emergency_stop = reconn_local.hget('safely_delete_key', 'emergency_stop')
                    emergency_stop = int(emergency_stop)
                except Exception, e:
                    emergency_stop = 0
                if emergency_stop == 1:
                    return (-1, 'Emergency stop,please check manually!')
                #print 'hash commit!'
                redis_pipe.execute()
                commit_flag=0
            redis_pipe.hdel(keyname,item)
            commit_flag+=1
            write_log_flag+=1
            finished_key_number+=1
        redis_pipe.execute()
        del field_list
        if finished_key_number > key_len:
            finished_key_number = key_len
        try:
            cur_myconn.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(task_id, u'任务执行中', 'info', str(finished_key_number) + '/' + str(key_len), createuser))
            myconn.commit()
        except Exception,e:
            print Exception,e
        try:
            reconn.delete(keyname)
        except Exception,e:
            cur_myconn.close()
            print Exception,e
            return (-1, 'Something wrong has happend,please check manually!')
        redis_pipe.execute()
        cur_myconn.close()
        print 'hash keys deleted,I will retrun the result'
        return (1, 'The key ' + keyname + ' has been deleted successfully!')
    else:
        print keyname,'has notype,please check manually!'
        return (-1,keyname+' has notype,please check manually!')
def sub_main():
    current_date=getdate()
####connect tasks server,get task info
    myconn_redis_mgr_flag,myconn_redis_mgr = get_database_connections(host='',port=,db='', user=user_serveradmin,password=passwd_serveradmin, dbtype='mysql')
    if myconn_redis_mgr_flag == -1:
        print getdate(), 'Main function: connects datatransfer_control fails'
        sys.exit()
    cur_myconn_redis_mgr=myconn_redis_mgr.cursor(my.cursors.DictCursor)
    sql_getinformation="select * from sqltoolsdb.tasks_form where run_appname='redis_mgr' and status='ready' order by id limit 1"
    cur_myconn_redis_mgr.execute(sql_getinformation)
    tt=cur_myconn_redis_mgr.fetchone()
    if  tt:
        cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='Running' where id=" + str(tt['id']))
        myconn_redis_mgr.commit()
        #cur_myconn_redis_mgr.close()
        #myconn_redis_mgr.close()
        task_info_tt = eval(tt['args_json'])
        redis_clustername=task_info_tt['redis_clustername']
        ##redis_port=task_info_tt['redis_port']
        redis_switcher=task_info_tt['redis_switcher']
        keyname=task_info_tt['keyname']
        createuser=tt['creator']
        sql_get_redis_cluster_info = "select a.name,b.ip,b.port,b.parentid from redisadmin_rediscluster a,redisadmin_redisinstance b where a.pool_id=b.pool_id and a.name='" + redis_clustername + "' and parentid=0 and b.status=1; "
        myconn_redis_info_flag, myconn_redis_info = get_database_connections(host='',port=, db='',user=user_serveradmin,password=passwd_serveradmin,dbtype='mysql')
        if myconn_redis_info_flag == -1:
            print getdate(), 'Main function: connects datatransfer_control fails'
            sys.exit()
        cur_myconn_redis_info = myconn_redis_info.cursor(my.cursors.DictCursor)
        cur_myconn_redis_info.execute(sql_get_redis_cluster_info)
        tt_cluster_info = cur_myconn_redis_info.fetchall()
        isexist=0
        redis_host=''
        redis_port=''
        for cluster_info_item in tt_cluster_info:
            try:
                reconn_remote=redis.Redis(host=cluster_info_item['ip'],port=cluster_info_item['port'])
                isexist=reconn_remote.exists(keyname)
            except Exception,e:
                print Exception,e
                #return (-1, 'Error in finding the key'+keyname+',please check manually!')
            if isexist:
              redis_host=cluster_info_item['ip']
              redis_port=cluster_info_item['port']
              break
        if not isexist:
            print keyname, 'doesnot exists,plsease check manually!'
            try:
                cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务结束', 'info', keyname + u' 不存在，请人工检查', createuser))
                cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='error' where id=" + str(tt['id']))
                myconn_redis_mgr.commit()
            except Exception, e:
                print Exception, e
            return (-1, keyname+'doesnot exists,plsease check manually!')
        if int(redis_switcher)==0:
            print 'keyname:',keyname
        try:
            reconn_romote=redis.Redis(host=redis_host,port=int(redis_port))
        except Exception,e:
            print Exception,e
            cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务结束', 'info', u'无法连接 redis server ,请人工检查', createuser))
            cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='error' where id=" + str(tt['id']))
            myconn_redis_mgr.commit()
            return (-1, 'Cannot connect redis server,please check manually!')
###########get information of slave nodes
        slave_list=get_slave_nodes(reconn_romote.info('replication'))
        if not slave_list:
            slave_ip=redis_host
            slave_port=redis_port
        else:
            slave_ip=slave_list[-1]['ip']
            slave_port=slave_list[-1]['port']
        try:
            reconn_remote_slave=redis.Redis(host=slave_ip,port=int(slave_port))
        except Exception,e:
            print Exception,e
            cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务结束', 'info', u'无法连接 redis server ,请人工检查', createuser))
            cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='error' where id=" + str(tt['id']))
            myconn_redis_mgr.commit()
            cur_myconn_redis_mgr.close()
            myconn_redis_mgr.close()
            return (-1, 'Cannot connect redis server,please check manually!')
#### ######## check weither delete one key of many keys.
        if int(redis_switcher)==0:
            isexist=reconn_remote_slave.exists(keyname)
            if not isexist:
                print keyname,'doesnot exists,plsease check manually!'
                try:
                    cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务结束', 'info',keyname+u' 不存在，请人工检查', createuser))
                    myconn_redis_mgr.commit()
                except Exception,e:
                    print Exception,e
            else:
                del_result=delete_key(myconn_redis_mgr,reconn_remote_slave,reconn_romote,keyname,tt['id'],createuser)
                if del_result[0]==1:
                    print del_result[1]
                    try:
                        cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='success' where id=" + str(tt['id']))
                        cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务完成', 'info',keyname+u'已被成功删除',createuser))
                        myconn_redis_mgr.commit()
                    except Exception,e:
                        print Exception,e
                else:
                    print 'Something wrong has happened,please check manually'
                    try:
                        cur_myconn_redis_mgr.execute("update sqltoolsdb.tasks_form set  status='error' where id=" + str(tt['id']))
                        cur_myconn_redis_mgr.execute("insert into tasks_formlog(formid,operationtype,level,operationdetail,creator)values(%s,%s,%s,%s,%s)",(tt['id'], u'任务错误', 'info',del_result[1] ,createuser))
                        myconn_redis_mgr.commit()
                    except Exception,e:
                        print Exception,e

######delete all keys in a redis instance,
#    remote_slave_available_memory=get_availabe_memroy_os_debug(slave_ip)
#    if remote_slave_available_memory==-1:
#        print 'Cannot get the memory info of remote server,please check manually!'
#        sys.exit()
#    print '######## Now do the remote server memory check!'
#    sleep(1)
#    redis_used_memory=reconn_remote_slave.info('memory')['used_memory_rss']/1024
#    if remote_slave_available_memory>redis_used_memory:
#        print 'Memory is big enough for a new slave'
#        slave_add=reconn_local.execute_command("slaveof "+slave_ip+" "+str(slave_port))
#        print 'slave_add:',slave_add
#        if slave_add=='OK':
#            wait_counts=0
#            while True:
#                if wait_counts==5000:
#                    print 'It takse so long time to start the replication,please check manually!'
#                    sys.exit()
#                if reconn_local.info('replication')['master_link_status']=='up':
#                    print 'Sync sucessfully!'
#                    break
#                else:
#                    print 'Syncing!,I will sleep 10 seconds'
#                    sleep(10)
#                wait_counts+=1
#        else:
#            print 'Cant add slave of ',slave_ip
#            sys.exit()
#    else:
#        print 'Memory is not big enough for a new slave,please do the key deleting manually!'
#        sys.exit()
#
#    slave_delete=reconn_local.execute_command("slaveof no one")
#    if slave_delete=='OK':
#        print 'Local machine has become a master!'
#    keylist=get_key_list(reconn_local)
#    delete_nottl_keys=get_nottl_keys(reconn_local, keylist)
#    nottl_key_num=len(delete_nottl_keys)
#    percent_num=nottl_key_num/5
#    finished_num=0
#    delete_count=0
#    print 'About',nottl_key_num,' keys will be deleted'
#    for item in delete_nottl_keys:
#        if delete_count==finished_num:
#            print 'Processing ',str(finished_num*100.0/nottl_key_num)+'%'
#            finished_num+=percent_num
#        delete_count+=1
#        if reconn_romote.ttl(item):
#            print item,'has ttl time,cannot be deleted,please check manually!'
#            continue
#        delete_result=delete_key(reconn_local,reconn_romote,item)
#    print 'All keys have ben deleted successfully!'
##print('hello')
    else:
        print current_date,'I am alive,but no work to do!\n'
    cur_myconn_redis_mgr.close()
    myconn_redis_mgr.close()
def main():
    while True:
        try:
            sub_main()
        except Exception,e:
            print Exception,e
        sleep(5)
if __name__ == '__main__':
    main()