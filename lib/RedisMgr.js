var logger = require('pomelo-logger').getLogger("RedisMgr", __filename);
var dbTable = require("./DBTable");
var util = require('util');

/**
 * redis缓存工具类</br>
 * 用于缓存表数据,减轻数据的查询压力.</br>
 * 该工具类初始化于DataMgr,供DataMgr使用.</br>
 *
 * 功能简介:</br>
 *      查询缓存数据,更新/新增缓存数据.并设置数据过期时间.</br>
 * date:17/5/8
 * @author wuqingkai
 */
var RedisMgr = module.exports;
/**
 * 初始化
 * @param redisInfo {JSON} redis配置信息
 */
exports.init = (redisInfo)=>{
    if (!redisInfo || !redisInfo['host'] || !redisInfo['port'])
        return;

    exports.redisClient = require("ioredis").createClient(redisInfo['port'], redisInfo['host'], redisInfo['password']);
    exports.redisClient.on("error", function (err) {
        logger.log("Error " + err);
    });
    //redisClient.select(1);
};

//var TABLE_NAME              = "tableName";
//var COLUMN                  = "column";
//var COLUMN_NAME             = "columnName";
//var COLUMN_NAMES            = "columnNames";
//var COLUMN_DEFAULT_VALUE    = "columnDefaultValue";
var PRIMARY_KEY             = "primaryKey";
var FOREIGN_KEY             = "foreignKey";
var SON_KEY                 = "sonKey";
//var AUTO_INCREMENT          = "autoIncrement";

/**
 * 缓存数据
 *
 * hash
 * 没有外键的表用hash结构缓存.
 * hash的双key为表名和主键值
 *      tableName, primaryKey
 *
 * List
 * 有外键的表用List结构缓存.
 *      表名:外键值
 *      tableName:foreignKey
 *
 *
 * @param tableName {string}            表名
 * @param toSet     {obj||objArray}     将要设置缓存的数据(单条或多条),
 *                          每条数据均是对象,结构如下:
 *                          {
 *                              tableName   :   "player",       //表名
 *                              json        :   "dataForCache"  //要缓存数据的json对象
 *                          }
 *
 * @param cb
 */
exports.addRedisCache = function(tableName, toSet, cb) {
    if (!tableName || !toSet) {
        cb(`params null`);
        return;
    }
    var table = dbTable.tables.get(tableName);
    if (!!table[FOREIGN_KEY]) {
        // list
        var redisOrder = [];
        redisOrder.push('rpush');
        if (util.isArray(toSet)) {
            redisOrder.push(tableName + ":" + toSet[0][table[FOREIGN_KEY]]);
            toSet.forEach((data)=>{
                redisOrder.push(JSON.stringify(data));
            });
        } else {
            redisOrder.push(tableName + ":" + toSet[table[FOREIGN_KEY]]);
            redisOrder.push(JSON.stringify(toSet));
        }
        exports.redisClient.pipeline([redisOrder]).exec((err, data)=>{
            cb(err, data);
        });
    } else {
        // map
        var pipeArr = [];
        if (util.isArray(toSet)) {
            toSet.forEach((data)=>{
                pipeArr.push(['hset', tableName, data[table[PRIMARY_KEY]], JSON.stringify(data)]);
            });
        } else {
            pipeArr.push(['hset', tableName, toSet[table[PRIMARY_KEY]], JSON.stringify(toSet)]);
        }
        exports.redisClient.pipeline(pipeArr).exec((err, data)=>{
            cb(err, data);
        });
    }
};

/**
 *
 * 获取缓存数据
 * @param tableName
 * @param sign
 * @param cb
 */
exports.getRedisCache = function(tableName, sign, cb) {
    var table = dbTable.tables.get(tableName);
    if (!!table[FOREIGN_KEY]) {
        // list
        var cacheKey = tableName + ":" + sign;
        exports.redisClient.lrange(cacheKey, 0, -1, cb);
    } else {
        // map
        exports.redisClient.hget(tableName, sign, cb);
    }
};

/**
 * 获取list中某下标的数据
 * @param tableName
 * @param sign
 * @param index
 * @param cb
 */
exports.getRedisCacheByIndex = function(tableName, sign, index, cb) {
    var cacheKey = tableName + ":" + sign;
    exports.redisClient.lindex(cacheKey, index, cb);
};

exports.updateRedisCache = function(tableName, json, index, cb) {
    if (!!index && typeof index === 'function') {
        cb = index;
        index = null;
    } else if (!cb) {
        logger.error("updateRedisCache mast has cb");
        return;
    }
    if (!tableName || !json) {
        cb("updateRedisCache failed :: param is null");
        return;
    }

    var table = dbTable.tables.get(tableName);
    if (!table) {
        logger.error(`updateRedisCache :: can not find table by data::${tableName}`);
        return;
    } else if (!table[PRIMARY_KEY]) {
        logger.error(`updateRedisCache :: table has no pri key by data::${tableName} `);
        return;
    }
    if (!!table[FOREIGN_KEY]) {
        if (isNaN(index)) {
            cb(`table has foreignKey mast has index!::${tableName}`);
            return;
        }
        // list
        var cacheKey = tableName + ":" + json[table[FOREIGN_KEY]];
        exports.redisClient.lset(cacheKey, index, JSON.stringify(json), cb);
    } else {
        // map
        exports.redisClient.hset(tableName, json[table[PRIMARY_KEY]], JSON.stringify(json), cb);
    }
};

exports.removeCacheByValue = function(tableName, foreignValue, toRem) {
    var key = tableName + ":" + foreignValue;
    if (util.isArray(toRem)) {
        var pipeArr = [];
        toRem.forEach((v)=>{
            pipeArr.push(['lrem', key, 0, v]);
        });
        exports.redisClient.pipeline(pipeArr).exec((err)=>{
            if (!!err) logger.error(`rem failed::${err}`);
        });
    } else {
        exports.redisClient.lrem(key, toRem, (err)=>{
            if (!!err) logger.error(`rem failed::${err}`);
        })
    }
};


/**
 * 根据根表和根表主键值,删除其和其下相关的数据缓存.
 * @param tableName
 * @param primaryValue
 * @param foreignValue
 */
exports.deleteRedisCacheByFather = function(tableName, primaryValue, foreignValue) {
    if (!tableName || !primaryValue) {
        logger.error('delete cache failed::params is null');
        return;
    }

    var table = dbTable.tables.get(tableName);
    if (!table) {
        logger.error(`delete cache failed:: can not find table by tableName::${tableName}`);
        return;
    //} else if (!!table[FOREIGN_KEY]) {
    //    logger.error(`delete cache failed:: can not delete cache by sonTable::${tableName}`);
    //    return;
    }

    if (!table[SON_KEY])
        return;

    var pipeArr = [];
    // 删除自身数据
    if (!!table[FOREIGN_KEY]) {
        pipeArr.push(['del', tableName + ":" + foreignValue]);
    } else {
        pipeArr.push(['hdel', tableName, primaryValue]);
    }

    var getSonOrder = function(tName, primaryValue, cb) {
        var theTable = dbTable.tables.get(tName);
        if (!theTable[SON_KEY]) {
            cb();
            return;
        }
        var sonNames = theTable[SON_KEY];
        var sonPipe = [], sonCacheKey = "";
        sonNames.forEach((sonN)=>{
            sonCacheKey = sonN + ":" + primaryValue;
            sonPipe.push(["lrange", sonN + ":" + primaryValue]);
            pipeArr.push(['del', sonCacheKey]);
        });

        exports.redisClient.pipeline(sonPipe).exec((err, data)=>{
            if (!!err) {
                logger.error(`delete cache failed!::${err}`);
                return;
            }
            var length = 0;
            data.forEach((aData)=>{
                if (!aData || !aData.length)
                    return;
                length += aData.length;
            });

            var count = 0;
            var checkEnd = function() {
                ++count === length && cb();
            };

            var sonN = "", aSonT = null;
            data.forEach((aData, idx)=>{
                if (!aData || !aData.length)
                    return;
                sonN = sonNames[idx];
                aSonT = dbTable.tables.get(sonN);
                aData.forEach((aSonData)=>{
                    getSonOrder(sonN, aSonData[aSonT[PRIMARY_KEY]], checkEnd);
                });
            });

        });
    };

    getSonOrder(tableName, primaryValue, ()=>{
        exports.redisClient.pipeline(pipeArr).exec((err, data)=>{if (!!err) logger.error(err)});
    });
};

/**
 * 执行lua脚本
 * @param lua
 * @param paramNum
 * @param keysArray
 * @param paramsArray
 * @param cb
 */
exports.runLua = function(lua, paramNum, keysArray, paramsArray, cb) {
    if (typeof paramsArray == 'function') {
        cb = paramsArray;
        paramsArray = keysArray;
    }
    exports.redisClient.eval(lua, paramNum, keysArray, paramsArray, (err, res)=>{
        cb(err, res);
    });
};