# redis_management_tools
##########对线上redis 群集进行管理和维护###################

1.现状

  a.一个group包含至少一个master+slave，以group单位管理

  b.目前线上有近上万个groups需要管理

2.目前还有如下功能

  a.slowlog 采集:
  
    采用并发架构和slowlog 增量抽取的方式，保证线上所有实例的slowlog采集
	
  b.线上redis群集配置检测
  
    线上由于变更或非标准操作导致线上的群集配置异常，因此需要日常的配置检测，目前检测项如下 master，slave 在非同一IDC，master，sentinel在非同一IDC，master缺少sentinel监控
	
	redis group下面但sentinel 配置未删除，master受多个sentinel监控等
	
  c.big key删除
  
    redis在4.0以下版本，big key的删除是会导致主进程的阻塞，进而导致生产事故。该工具可以优雅的删除big key。原理为针对不同的类型，小批量的移除该类型key中的元素，直至所有的元素被移除完之后，在删除该key即可，
	
	如 hash key，小批量循环移除filed和filed value，然后删除hash key。

