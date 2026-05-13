### FlinkCDC kingbase人大金仓数据库监听


#### 修改数据库配置文件
执行语句 SHOW wal_level，查看返回值是否为'logical'，否则修改kingbase.conf中wal_level=logical


#### 如果需要监听修改前数据，需要对表执行以下语句
ALTER TABLE test_001 REPLICA IDENTITY FULL;

发现有些表不执行上面这个语句的话，更新数据会报错（好像是没有主键的表才会这样？）：
UPDATE test_001 SET serial_no = '3'
> ERROR:  cannot update table "test_001" because it does not have a replica identity and publishes updates
HINT:  To enable updating the table, set REPLICA IDENTITY using ALTER TABLE.


原文：https://blog.csdn.net/weixin_43966292/article/details/142255849
