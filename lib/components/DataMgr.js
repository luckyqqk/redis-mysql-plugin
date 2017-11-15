var async = require('async');
var util = require('util');
var logger = require('pomelo-logger').getLogger('pomelo', __filename);

var RedisMgr = require("./bean/RedisMgr");
var DBMgr = require("./bean/DBMgr");
var DBTable = require("./bean/DBTable");

/**
 * 数据缓存和持久化的插件,该插件包装了redis和mysql.</br>
 * 作用于让逻辑工程师不再关心数据的缓存以及持久化.</br>
 * 使用方法:</br>
 *      1.确保mysql插件,RedisMgr.js,DBMgr.js,DBTable.js在同级目录下.</br>
 *      2.在config中增加mysql.json和redis.json</br>
 *      3.在app.js初始化后端服务器时挂载此插件</br>
 *
 * 此插件主要提供getData/setData/insertData方法来获取/更新和插入数据.</br>
 * 对于逻辑工程师:</br>
 *  需要数据的时候只需要调用     pomelo.app.get(dataMgr).getData()方法即可.</br>
 *  需要存储数据的时候只需要调用  pomelo.app.get(dataMgr).setData()方法即可.</br>
 *  需要插入并得到插入后数据时调  pomelo.app.get(dataMgr).insertData()即可.</br>
 * 方法参数基本相似:数据库表名和json参数.</br>
 * date:16/12/2
 * @author wuqingkai
 */

module.exports = function(app, opts) {
    var dataMgr = new DataMgr(app, opts);
    app.set('dataMgr', dataMgr, true);
    return dataMgr;
};

var DataMgr = function(app, opts) {
    this.name = 'dataMgr';
    this.app = app;
    this.opts = opts;
};

var pro = DataMgr.prototype;

/**
 * 插件初始化
 * 1.初始化RedisMgr
 * 2.初始化DBMgr
 * 3.初始化DBTable
 * @param cb
 */
pro.start = function(cb) {
    RedisMgr.init(this.opts);
    var app = this.app;
    app.loadConfig('mysqlInfo', app.getBase() + '/config/mysql/mysql.json');
    var mysqlInfo = app.get("mysqlInfo");
    var databaseName = mysqlInfo.database;
    var dbClient = require('./mysql/mysql').init(app);//配置数据库
    DBMgr.init(dbClient);
    DBTable.init(dbClient, databaseName, ()=>{
        //logger.debug(`${app.getServerId()} dataMgr startup`);
        cb();
    });
};
var PRIMARY_KEY             = "primaryKey";
var FOREIGN_KEY             = "foreignKey";
var SON_KEY                 = "sonKey";
var REDIS_INDEX             = "redisIndex";

/**
 * 取某个表中sign相关的所有数据.
 *                  (有外键的表,取出的数据是个数组,否则是单条数据).
 * @param tableName 表名
 * @param sign      有外键的表填外键值,否则填主键值
 * @param cb
 */
pro.getTableData = function (tableName, sign, cb) {
    if (!cb) {
        logger.error("getTableData mast has cb");
        return;
    } else if (!tableName || !sign) {
        cb("getTableData failed :: param is null");
        return;
    }
    RedisMgr.getRedisCache(tableName, sign, (err, data)=>{
        if (!!err) {
            cb(err);
            return;
        }
        if (!data || (util.isArray(data) && data.length < 1)) {
            // load from db
            var sql = DBTable.getSelectSql(tableName, sign);
            if (!sql) {
                cb(null, data);
                logger.error(`cache failed::sql is null`);
                return;
            }
            //console.error(sql);
            DBMgr.query(sql, [], function (err, dataDB) {
                if (!!err) {
                    cb(err);
                } else if (!dataDB || dataDB.length < 1) {
                    //if (!DBTable.tables.get(tableName)[FOREIGN_KEY])
                    //    cb();
                    //else
                    cb();
                } else {
                    dataDB = JSON.parse(JSON.stringify(dataDB));
                    if (!DBTable.tables.get(tableName)[FOREIGN_KEY])
                        dataDB = dataDB[0];
                    RedisMgr.addRedisCache(tableName, dataDB, (err)=>{
                        if (!!err) logger.error(`addRedisCache failed::${err}`);
                        cb(err, dataDB);
                    });
                }
            });
        } else {
            if (util.isArray(data)) {
                var result = [];
                data.forEach((aData)=>{
                    result.push(JSON.parse(aData));
                });
                cb(null, result);
            } else {
                cb(null, JSON.parse(data));
            }
        }
    });
};

/**
 * 根据条件取数据
 * @param tableName 表名
 * @param condition 返回符合条件的数据,必须含有主键值或者外键值
 * @param cb
 */
pro.getTableDataByCondition = function (tableName, condition, cb) {
    var self = this;
    var table = DBTable.tables.get(tableName);
    var sign = condition[table[PRIMARY_KEY]];
    if (!!table[FOREIGN_KEY]) {
        sign = condition[table[FOREIGN_KEY]];
    }
    if (!sign) {
        cb('need primaryKey or foreignKey!');
        return;
    }
    self.getTableData(tableName, sign, (err, data)=>{
        if (!!err) {
            cb(err);
            return;
        } else if (!data) {
            cb();
            return;
        }
        var isInCondition = function(json) {
            for (let key in condition) {
                if (condition[key] != json[key])
                    return false;
            }
            return true;
        };
        var result = [];
        data.forEach((aData, idx)=>{
            if (!isInCondition(aData))
                return;
            aData[REDIS_INDEX] = idx;
            result.push(aData);
        });
        //result = result.length == 1 ? result[0] : result;
        cb(null, result);
    });
};

/**
 * 获取多表数据,返回数据顺序与请求数组顺序相同.
 * @param arr   {Array} 二维数组,第一维表示所取表的个数;第二维的数组长度为2,第一位表示表名,第二位在有外键的表时填外键值,否则填主键值.
 *                      如下:
 *                      [[tableName1, primaryValue],[tableName2, foreignValue]],[tableName3, {sign:xx, con:xx}]
 * @param cb
 */
pro.getDataByArray = function(arr, cb) {
    var self = this;
    var agent = function(tableName, sign) {
        return function(callback) {
            self.getTableData(tableName, sign, callback);
        };
    };
    var agentCondition = function(tableName, condition) {
        return function(callback) {
            self.getTableDataByCondition(tableName, condition, callback);
        };
    };
    var funcArr = [];
    arr.forEach((cdt)=>{
        if (util.isObject(cdt[1])) {
            funcArr.push(new agentCondition(cdt[0], cdt[1]));
        } else {
            funcArr.push(new agent(cdt[0], cdt[1]));
        }
    });
    async.parallel(funcArr, (err, data)=>{
        cb(err, data);
    });
};
////不推荐使用
//pro.getDataByConditionArray = function() {
//
//};

/**
 * 数据更新
 * @param tableName
 * @param jsonValue
 * @param redisIndex
 * @param cb
 */
pro.updateData = function(tableName, jsonValue, redisIndex, cb) {
    var table = DBTable.tables.get(tableName);
    jsonValue = DBTable.getAllTableJson(table, jsonValue);
    RedisMgr.updateRedisCache(tableName, jsonValue, redisIndex, (err)=>{
        if (!!err) {
            logger.error(`update cache failed::${err}`);
            cb(err);
            return;
        }
        // save db
        var sql = DBTable.getUpdateSqlByJson(tableName, jsonValue);
        if (!sql) {
            cb(null, data);
            logger.error(`update cache failed::sql is null`);
            return;
        }
        DBMgr.query(sql, [], function (err, data) {
            if (!!err) {
                logger.error(`updateData failed::${err}`);
                cb(err);
            } else
                cb(null, data);
        });
    });
};

/**
 * 删除数据,首先清除缓存,然后删除数据库.
 * @param tableName
 * @param foreignValue
 * @param index
 * @param cb
 */
pro.deleteTableData = function(tableName, foreignValue, index, cb) {
    if (index == undefined || index.length < 1) {
        cb(`deleteTableData failed :: index can not be null`);
        return;
    }
    var table = DBTable.tables.get(tableName);
    if (!table[FOREIGN_KEY]) {
        cb(`delete failed:: table::${tableName} has no foreignKey!`);
        return;
    }
    var agent = function(tn, fv, idx) {
        var getCache = function(cbk) {
            RedisMgr.getRedisCacheByIndex(tn, fv, idx, cbk);
        };
        return getCache;
    };
    var funcArr = [];
    if (util.isArray(index)) {
        index.forEach((idx)=>{
            funcArr.push(new agent(tableName, foreignValue, idx));
        });
    } else {
        funcArr.push(new agent(tableName, foreignValue, index));
    }
    async.parallel(funcArr, (err, data)=>{
        if (!!err) {
            cb(err);
            return;
        }
        async.parallel([
            (_cb)=>{
                // delete cache
                RedisMgr.removeCacheByValue(tableName, foreignValue, data, _cb);
            },
            (_cb)=>{
                // delete db
                var sql = "";
                if (util.isArray(data)) {
                    var dArr = [];
                    data.forEach((aData)=>{
                        dArr.push(JSON.parse(aData));
                    });
                    sql = DBTable.getDeleteSql(tableName, dArr);
                } else {
                    sql = DBTable.getDeleteSql(tableName, JSON.parse(data));
                }
                //console.error(sql);
                DBMgr.query(sql, [], _cb);
            }
        ], (err)=>{
            cb(err);
        });
    });
};

/**
 * 根据外键值删除数据
 * @param tableName
 * @param foreignValue
 * @param cb
 */
pro.deleteTableDataByForeignValue = function(tableName, foreignValue, cb) {
    var sql = DBTable.getDeleteSqlByForeign(tableName, foreignValue);
    if (!sql) {
        cb(`deleteTableDataByForeignValue failed:: table::${tableName} has no foreignKey!`);
    } else {
        async.parallel([
            (_cb)=>{
                RedisMgr.removeCacheByKey(tableName, foreignValue, _cb);
            },
            (_cb)=>{
                DBMgr.query(sql, [], _cb);
            }
        ], (err)=>{
            if (!!err) {
                logger.error(`deleteTableDataByForeignValue from db failed::${err}`);
            }
            cb(err);
        });
    }
};

/**
 * 数据插入数据库,并缓存到redis<br/>
 * 若非同表,是无法批量的,也没有意义.所以,该方法仅支持同一张表,一起插入多条数据.
 * @param tableName {String} 表名
 * @param jsonArray {JSON} 想要插入的数据.(可是数组,也可是单条数据,若传入的是个数组, 则会批量插入,也会返回一个数组.)<br/>(只需传入必须字段键值对,其他字段程序以默认值的方式补全)
 * @param cb {function}   返回含有全部字段键值对(包括主键)
 */
pro.insertData = function (tableName, jsonArray, cb) {
    if (!tableName || !jsonArray || jsonArray.length < 1) {
        logger.error("insertData param is null");
        return;
    }
    var table = DBTable.tables.get(tableName);
    if (!table) {
        cb(`DataMgr.insertData fail :: can not find table by tableName:${tableName}`);
        return;
    }
    var allJsonArray = DBTable.getAllInsertJson(table, jsonArray);
    // 组建sql,自带批量处理.
    var sql = DBTable.getInsertSqlByJson(table, allJsonArray);
    if (!sql) {
        logger.error("insertData sql err by json :: " + allJsonArray);
        return;
    }
    DBMgr.query(sql, [], function (err, data) {
        if (!!err) {
            cb(err);
            logger.error("DBMgr query err : " + err);
            logger.error("sql : " + sql);
        } else if (!data) {
            cb('has no db data');
        } else {
            var insertId = data.insertId;
            if (util.isArray(allJsonArray)) {
                for (var idx in allJsonArray) {
                    allJsonArray[idx][table[PRIMARY_KEY]] = insertId++;
                }
            } else {
                allJsonArray[table[PRIMARY_KEY]] = insertId;
            }
            RedisMgr.addRedisCache(tableName, allJsonArray, (err)=>{
                if (!!err) {
                    cb('addRedis err');
                    logger.error(`add cache failed::${err}`);
                } else
                    cb(null, allJsonArray);
            });
        }
    });
};

/**
 * 查看数据是否存在
 * @param tableName
 * @param condition 返回符合条件的数据,必须含有主键值或者外键值
 * @param cb
 */
pro.isExist = function (tableName, condition, cb) {
    if (!tableName || !condition) {
        cb('tableName or condition can not be null');
        return;
    }
    var sql = "select * from `" + tableName + "` where ";
    for (let k in condition) {
        sql += "`" + k + "` = ";
        var value = isNaN(condition[k]) ? '"' + condition[k] + '"' : condition[k];
        sql += value;
        sql += " and "
    }
    sql = sql.substr(0, sql.length - 4);
    DBMgr.query(sql, [], function (err, dataDB) {
        if (!!err) {
            cb(err);
        } else if (!dataDB || dataDB.length < 1) {
            cb(null, false);
        } else {
            cb(null, true);
        }
    });
};

pro.getPrimaryKey = function(tableName) {
    var table = DBTable.tables.get(tableName);
    if (!table) return null;
    else return table[PRIMARY_KEY];
};

/**
 * 根据根表和根表主键值,删除其和其下相关的数据缓存.
 * @param tableName     父级表名
 * @param primaryValue  必填
 * @param foreignValue  没外键不用添
 * @param cb
 */
pro.deleteRedisCacheByFather = function(tableName, primaryValue, foreignValue, cb) {
    RedisMgr.deleteRedisCacheByFather(tableName, primaryValue, foreignValue, cb);
};

/**
 * redis执行lua脚本,无数据库操作.
 * @param lua
 * @param paramNum
 * @param keysArray
 * @param paramsArray
 * @param cb
 */
pro.runLua = function(lua, paramNum, keysArray, paramsArray, cb) {
    RedisMgr.runLua(lua, paramNum, keysArray, paramsArray, cb);
};