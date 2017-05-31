/**
 * Created by wuqingkai on 17/4/1.
 */
var DataMgr = require("../DataMgr");

module.exports = function(app, opts) {
    DataMgr.init(app, opts);
    app.set('dataMgr', DataMgr, true);
    return DataMgr;
};

