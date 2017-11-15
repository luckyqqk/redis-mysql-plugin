/**
 * 数据库sql的执行和后期扩展</br>
 * date:16/12/5
 * @author wuqingkai
 * @todo 将来做分库匹配,暂未想好分库实现.目前均直连数据库,若遇瓶颈,请增加缓存更新以及批量更新.
 */
var DBMgr = module.exports;
/**
 * @param dbClient {object} sql的执行对象
 */
exports.init = (dbClient)=>{
    exports.query = function(sql, valueArray, cb) {
        dbClient.query(sql,valueArray, cb);
    };
};