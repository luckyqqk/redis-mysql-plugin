/**
 *
 */
// mysql CRUD
var sqlclient = module.exports;

var _pool;

var NND = {};

NND.init = function(app){
    _pool = require('./dao-pool').createMysqlPool(app);
};

/**
 * 执行sql语句
 * @param {String} sql Statement The sql need to excute.
 * @param {Object} args The args for the sql.
 * @param {fuction} cb Callback function.
 *
 */
NND.query = function(sql, args, cb){
    _pool.acquire(function(err, client) {
        if (!!err) {
            console.error('[sqlqueryErr] '+err.stack);
            return;
        }
        client.query(sql, args, function(err, res) {
            _pool.release(client);
            cb(err, res);
        });
    });
};

/**
 * 关闭连接池
 */
NND.shutdown = function(){
    _pool.destroyAllNow();
};

/**
 * 初始化数据库
 */
sqlclient.init = function(app) {
    if (!!_pool){
        return sqlclient;
    } else {
        NND.init(app);
        sqlclient.insert = NND.query;
        sqlclient.update = NND.query;
        sqlclient.delete = NND.query;
        sqlclient.query = NND.query;
        return sqlclient;
    }
};

/**
 * 关闭数据库连接
 */
sqlclient.shutdown = function(app) {
    NND.shutdown(app);
};