var async           = require("async");
var logger          = require('pomelo-logger').getLogger('DBTable', __filename);

/**
 * 表结构关系工具类</br>
 * 收录某库全部表信息,外键关联信息.以及组建CRUD语句.</br>
 * 该工具在DataMgr中初始化,供DataMgr和RedisMgr使用.</br>
 * date:16/12/5
 * @author wuqingkai
 */
var DBTable = module.exports;
exports.tables = new Map();

var TABLE_NAME              = "tableName";
var COLUMN                  = "column";
var COLUMN_NAME             = "columnName";
var COLUMN_NAMES            = "columnNames";
var COLUMN_DEFAULT_VALUE    = "columnDefaultValue";
var PRIMARY_KEY             = "primaryKey";
var FOREIGN_KEY             = "foreignKey";
var SON_KEY                 = "sonKey";
var AUTO_INCREMENT          = "autoIncrement";
var SPLIT = ":", REFERENCED = "refer";

//用于生成表的默认json数据</br>
//node均使用json数据格式.这些列类型对应的数据是number,剩下的均用string.
//@type {string[]}
var NUMBER_COLUMN_TYPES     = ["tinyint", "smallint", "mediumint", "int", "bigint", "float", "double", "decimal"];

/**
 * 初始化
 * @param dbClient {object} 执行sql的对象
 * @param databaseName {string} 数据库名
 */
exports.init = function (dbClient, databaseName) {
    dbClient.query("show tables;", [], function (err, data) {
        if (!!err) {
            logger.error(`DBTable init err ${err}`);
        } else if (!data || data.length < 1) {
            logger.error("DBTable init err:: can not find any table");
        } else {
            var agent = function(tableName) {
                var getDesc = function(cb) {
                    dbClient.query(`describe ${tableName}`, [], (err, columns)=> {

                        if (!!err) {
                            logger.error(`describe ${tableName} err::${err}`);
                        } else {
                            //{
                            //    Field: 'ID',
                            //    Type: 'int(11)',
                            //    Null: 'NO',
                            //    Key: 'PRI',
                            //    Default: null,
                            //    Extra: 'auto_increment' },
                            var table = {};
                            table[TABLE_NAME] = tableName;
                            var tableColumns = [];
                            var columnNames = [];
                            for (var col of columns) {
                                if(!col || typeof(col) == "function")
                                    continue;
                                var columnMap = {};
                                columnMap[COLUMN_NAME] = col["Field"];
                                if (col["Key"] == "PRI") {
                                    if (col["Field"].indexOf(SPLIT) != -1 && col["Field"].split(SPLIT)[0] == REFERENCED) {
                                        table[FOREIGN_KEY] = col["Field"];
                                    } else {
                                        table[PRIMARY_KEY] = col["Field"];
                                        columnMap[PRIMARY_KEY] = 1;
                                    }
                                }
                                if (col["Extra"] == 'auto_increment')
                                    columnMap[AUTO_INCREMENT] = 1;

                                columnNames.push(col["Field"]);
                                var colType = col["Type"].substr(0, col["Type"].indexOf("("));
                                if (NUMBER_COLUMN_TYPES.indexOf(colType) != "-1")
                                    columnMap[COLUMN_DEFAULT_VALUE] = 0;
                                else
                                    columnMap[COLUMN_DEFAULT_VALUE] = "-";
                                tableColumns.push(columnMap);
                            }
                            table[COLUMN] = tableColumns;
                            table[COLUMN_NAMES] = columnNames;
                            exports.tables.set(tableName, table);
                        }

                        cb();
                    });
                };
                return getDesc;
            };
            var funcArray = [];
            var tableKey = `Tables_in_${databaseName}`;
            for (var v of data) {
                if (!v || typeof(v) == "function")
                    continue;
                var tableName = v[tableKey];
                funcArray.push(new agent(tableName));
            }
            async.parallel(funcArray, ()=>{
                logger.log("DBTable desc over!");
                loadForeignTable();
            });
        }
    });
    var loadForeignTable = function() {
        dbClient.query(`select * from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_SCHEMA='${databaseName}' and REFERENCED_TABLE_NAME is not null`, [], (err, constraints)=> {
            if (!!err) {
                logger.error(`DBTable load constraints err::${err}`);
            } else {
                for (var v of constraints) {
                    if (!v || typeof(v) == "function")
                        continue;

                    var theTableName = v['TABLE_NAME'];
                    var theColumnName = v['COLUMN_NAME'];

                    //var fatherColumnName = constraints['REFERENCED_COLUMN_NAME'];

                    var tableSon = exports.tables.get(theTableName);
                    if (!tableSon) {
                        logger.warn(`can not find tableSon :: ${theTableName}`);
                        continue;
                    }
                    if (!tableSon[FOREIGN_KEY]) {
                        tableSon[FOREIGN_KEY] = theColumnName;
                    // 数据只需要一个爹, 暂不需要多外键联合键
                    //} else {
                        //tableSon[FOREIGN_KEY] += "+";
                        //tableSon[FOREIGN_KEY] += theColumnName;
                    }

                    var fatherTableName = v['REFERENCED_TABLE_NAME'];
                    var tableFather = exports.tables.get(fatherTableName);
                    if (!tableFather) {
                        logger.warn(`can not find tableFather :: ${theTableName}`);
                        continue;
                    }
                    var sonArray = tableFather[SON_KEY] = tableFather[SON_KEY] || [];
                    sonArray.push(theTableName);
                }
            }
            //console.error(exports.tables);
            logger.log("DBTable load foreign table over!");
            //{
            //    CONSTRAINT_CATALOG: 'def',
            //    CONSTRAINT_SCHEMA: 'main_bj',
            //    CONSTRAINT_NAME: 'player_id',
            //    TABLE_CATALOG: 'def',
            //    TABLE_SCHEMA: 'main_bj',
            //    TABLE_NAME: 'u_tank',
            //    COLUMN_NAME: 'uid',
            //    ORDINAL_POSITION: 1,
            //    POSITION_IN_UNIQUE_CONSTRAINT: 1,
            //    REFERENCED_TABLE_SCHEMA: 'main_bj',
            //    REFERENCED_TABLE_NAME: 'u_palyer',
            //    REFERENCED_COLUMN_NAME: 'ID' }
        });
    };
};

/**
 * 获得某表数据的默认JSON供插入需求使用.返回的JSON不会包含自增属性的字段.
 * @param table {object}    必填 数据库表对象
 * @param jsonArray {JSON}  必填 表数据键值对,只需传入必须字段,其他字段由程序自动默认添加<br/>
 *                          若传入的是一个数组,则也返回一个数组
 * @returns
 */
exports.getAllInsertJson = function(table, jsonArray) {
    if (!table || !jsonArray)
        return "";
    var getInsertJson = (table, json)=>{
        var insertJson = {};
        var cols = table[COLUMN];
        for (var col of cols) {
            if (col[AUTO_INCREMENT]) {
                continue;  // 不包含自增
            }
            var jsonKey = col[COLUMN_NAME];
            insertJson[jsonKey] = !!json[jsonKey] ? json[jsonKey] : col[COLUMN_DEFAULT_VALUE];
        }
        return insertJson;
    };
    // 单条的直接返回
    if (typeof jsonArray == 'object' && jsonArray.hasOwnProperty('length')) {
        var result = [];
        jsonArray.forEach((json)=>{
            result.push(getInsertJson(table, json));
        });
        return result;
    } else
        return getInsertJson(table, jsonArray);
};

/**
 * 生成插入sql
 * @param table {object}必填 数据库表对象
 * @param allJsonArray {JSON}  必填 需包含全部字段的键值对,示例:{nick:"Kai", age:"18"...}表示"(nick,age)values('Kai',18)"
 *                          若传入的是一个数组,则返回一个批量插入的sql
 * @returns String
 */
exports.getInsertSqlByJson = function (table, allJsonArray) {
    // make key
    var _makeKeys_ = (json)=>{
        var keys = '';
        for (var columnKey in json) {
            keys += "`";
            keys += columnKey;
            keys += "`";
            keys += ",";
        }
        return keys.slice(0, -1);
    };
    // make value
    var _makeValue_ = (json)=>{
        var params = "(";
        for (var key in json) {
            var v = json[key];
            if (isNaN(v)) v = '"' + v + '"';
            params += v;
            params += ",";
        }
        return params.slice(0, -1) + '),';
    };
    var keys = '', values='';
    if (typeof allJsonArray == 'object' && allJsonArray.hasOwnProperty('length')) {
        keys = _makeKeys_(allJsonArray[0]);
        allJsonArray.forEach((json)=>{
            values += _makeValue_(json);
        });
    } else {
        keys = _makeKeys_(allJsonArray);
        values = _makeValue_(allJsonArray);
    }
    if (!keys || !values)
        return '';
    var sql = "insert into `" + table[TABLE_NAME] + "` (";
    sql += keys;
    sql += ") values ";
    sql += values.slice(0, -1);
    sql += ";";
    return sql;
};
/**
 * 生成删除sql
 * @param table {object}必填 数据库表对象
 * @param conditionJson {JSON} 主键值
 * @returns String
 */
//exports.getDeleteSql = function (table, conditionJson) {
//    if (!table || !conditionJson)
//        return "";
//    var sql = "delete from `" + table.get(TABLE_NAME) + "` where ";
//    for (var k in conditionJson) {
//        if (table.get(COLUMN_NAMES).indexOf(k) == "-1") {
//            //logger.log(k + " can not find in table " + table.get(exports.TABLE_NAME));
//            continue;
//        }
//        sql += "`" + k + "` = ";
//        var v = conditionJson[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += " and ";
//    }
//    sql = sql.substr(0, sql.length - 4);
//    return sql;
//};
/**
 * 生成更新sql:更新列仅包含json中的key.where条件语句仅支持and连接.
 * @param table {object}必填 数据库表对象
 * @param json {JSON}   必填 示例:{nick:"Kai", age:"18"}表示"set nick='Kai',age=18"
 * @param conditionJson {JSON} 选填 更新条件.示例:{nick:"Kai", age:18}表示"where nick='Kai' and age=18".若不填则用主键作为条件 示例:"where id =1"
 * @returns String
 */
//exports.getUpdateSqlByJson = function (table, json, conditionJson) {
//    if (!table || !json)
//        return "";
//    var priArray = table.get(PRIMARY_KEY);
//    var isPri = (key)=>{
//        for (var pri of priArray) {
//            if (pri == key)
//                return true;
//        }
//        return false;
//    };
//    var sql = "update `" + table.get(TABLE_NAME) + "` set ";
//    for (var k in json) {
//        if (table.get(COLUMN_NAMES).indexOf(k) == "-1") {
//            logger.log(k + " can not find in table " + table.get(TABLE_NAME));
//            continue;
//        }
//        if (isPri(k)) {   // update语句不含主键
//            continue;
//        }
//        sql +=  '`' + k + '`';
//        sql += " = ";
//        var v = json[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += ",";
//    }
//    sql = sql.substr(0, sql.length - 1);
//    sql += " where ";
//    if (!conditionJson) {
//        for (var priK of priArray) {
//            if (!priK || typeof(priK) == "function")
//                continue;
//            sql +=  '`' + priK + '`';
//            sql += " = ";
//            sql += json[priK];
//            sql += " and ";
//        }
//    } else {
//        for (var key in conditionJson) {
//            sql +=  '`' + key + '`';
//            sql += " = ";
//            var v = conditionJson[k];
//            if (isNaN(v)) v = '"' + v + '"';
//            v = !v ? '""' : v;
//            sql += v;
//            sql += " and ";
//        }
//    }
//    return sql.substr(0, sql.length - 4);
//};
/**
 * 生成查询sql:仅提供条件and相接
 * @param table {object}必填 数据库表对象
 * @param conditionJson {JSON} 查询条件,示例:{nick:"Kai", age:18}表示"where nick='Kai' and age=18"
 * @returns String
 */
//exports.getSelectSql = function (table, conditionJson) {
//    if (!table)
//        return "";
//    var sql = "select * from `" + table.get(TABLE_NAME) + "` where ";
//    for (var k in conditionJson) {
//        sql += "`" + k + "`";
//        sql += " = ";
//        var v = conditionJson[k];
//        if (isNaN(v)) v = '"' + v + '"';
//        v = !v ? '""' : v;
//        sql += v;
//        sql += " and ";
//    }
//    sql = sql.substr(0, sql.length - 4);
//    return sql;
//};

exports.getSelectSql = function(tableName, sign) {
    var table = exports.tables.get(tableName);
    if (!table)
        return "";
    var sql = "select * from `" + tableName + "` where ";
    if (isNaN(sign)) sign = '"' + sign + '"';
    sign = !sign ? '""' : sign;
    var mainKey = table[FOREIGN_KEY] || table[PRIMARY_KEY];
    sql += "`" + mainKey + "` = ";
    sql += sign;
    return sql;
};


exports.getUpdateSqlByJson = function (tableName, json) {
    if (!tableName || !json)
        return "";
    var table = exports.tables.get(tableName);
    var priKey = table[PRIMARY_KEY];
    var priValue = json[priKey];
    if (!priKey)
        return "";
    if (isNaN(priValue))
        priValue = '"' + priValue + '"';
    var sql = "update `" + tableName + "` set ";
    for (var k in json) {
        if (table[COLUMN_NAMES].indexOf(k) == "-1") {
            logger.log(k + " can not find in table " + table[TABLE_NAME]);
            continue;
        }
        if (k == priKey) {   // update语句不含主键
            continue;
        }
        sql +=  '`' + k + '`';
        sql += " = ";
        var v = json[k];
        if (isNaN(v)) v = '"' + v + '"';
        v = !v ? '""' : v;
        sql += v;
        sql += ",";
    }
    sql = sql.substr(0, sql.length - 1);
    sql += " where ";
    sql += "`" + priKey + "` = ";
    sql += priValue;
    return sql;
};

exports.getDeleteSql = function(tableName, json) {
    if (!tableName || !json)
        return "";
    var table = exports.tables.get(tableName);
    var sql = "delete from `" + tableName + "` where ";
    if (typeof json == 'object' && json.hasOwnProperty('length')) {
        json.forEach((data)=>{
            sql += "`" + table[PRIMARY_KEY] + "` = ";
            sql += data[table[PRIMARY_KEY]];
            sql += " or "
        });
        sql = sql.substr(0, sql.length - 3);
    } else {
        sql += "`" + table[PRIMARY_KEY] + "` = ";
        sql += json[table[PRIMARY_KEY]];
    }
    return sql;
};