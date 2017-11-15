# redis-mysql-plugin
* 基于pomelo
* 按照缓存->数据库的优先级,增加/读取/更新/删除数据.让逻辑工程师不再需要考虑缓存和持久化.
-------------
### 使用方法
* 配置config下新建mysql和redis文件夹,文件夹下分别创建mysql.json和redis.json
```mysql.json
{
	"development": {
		"host" 		: "192.168.1.111",
		"port" 		: "3306",
		"database" 	: "xxxx",
		"user" 		: "yyyy",
		"password" 	: "111111"
	},
	"production": {
		"host" 		: "192.168.1.111",
		"port" 		: "3306",
		"database" 	: "xxxx",
		"user" 		: "yyyy",
		"password" 	: "111111"
	}
}
```
```redis.json{
  "DataMgr" : {
		"host" 		: "192.168.1.111",
		"port" 		: 6379,
		"password" 	: {}
	}
}
```
* app.js中require(DataMgr的文件位置),并在有需要的服务内加如下代码
```
app.use(DataMgr, require(app.getBase() + "/config/redis/redis.json"));
```
* 在需要获取数据的地方调用如下代码,即可获得数据.
```
pomelo.app.get('dataMgr').getTableData('表名', '外键值', (err, data)=>{
  // do something with data.
}
```
-------------------
### 方法支持
* 取某个表中sign相关的所有数据
```
getTableData
```
* 根据条件取数据
```
getTableDataByCondition
```
* 获取多表数据,返回数据顺序与请求数组顺序相同.
```
getDataByArray
```
* 插入数据
```
insertData
```
* 数据更新
``` 
updateData
```
* 数据删除
```
deleteTableData
```
* 某表某条件下数据是否存在,仅支持'='条件,不支持'<>'等条件.
```
isExist
```
* 根据根表和根表主键值,删除其和其下相关的数据缓存.
```
deleteRedisCacheByFather
```
* redis执行lua脚本,不会操作数据库.
```
runLua
```
**注释:updateData和deleteTableData中所需的redisIndex是从redis中get数据时数组的下标.条件获取的数据,会有redisIndex的字段. (难点)**
-----------------
### 缺陷
* 因配置比较繁琐,还未想出比较好的配置方案,且需伴随pomelo start启动,暂未支持npm
* 暂未增加延迟更新以及批量更新
