# redis-mysql-plugin
按照缓存->数据库的优先级,增加/读取/更新/删除数据.让逻辑工程师不再需要考虑缓存和持久化.基于pomelo

### 基于pomelo

### 使用方法
* 配置config.json
```json
{
  "init" : {
	"host" : "192.168.1.111",
	"port" : 6379,
	"password" : {}
  }
}
```
* app.js中在有需要的服务内加如下代码
* app.use(DataMgr, require(app.getBase() + "/config/redis/redis.json"));
* 在需要获取数据的地方调用如下代码,即可获得数据.
* pomelo.app.get('dataMgr').getTableData('表名', '外键值', (err, data)=>{
*   // do something with data.
* }
* 
### 方法支持
* getTableData                // 取某个表中sign相关的所有数据
* getTableDataByCondition     // 根据条件取数据
* getDataByArray              // 获取多表数据,返回数据顺序与请求数组顺序相同.
* insertData                  // 插入数据
* updateData = function(tableName, jsonValue, redisIndex)   // 数据更新
* deleteTableData = function(tableName, foreignValue, index, cb)
* 
* updateData中所需的redisIndex和deleteTableData中所需的index是get数据时数组的下标.条件获取的数据,会有redisIndex的字段. (难点)

* isExist                     // 某表某条件下数据是否存在,仅支持'='.
* deleteRedisCacheByFather    // 根据根表和根表主键值,删除其和其下相关的数据缓存.
* runLua                      // redis执行lua脚本,无数据库操作.

### 缺陷
* 暂未增加延迟更新以及批量更新
