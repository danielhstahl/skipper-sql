/**
 * Module dependencies
 */

var Writable = require('stream').Writable;
var _ = require('lodash');
var mssql = require('mssql');
module.exports = function msSqlStore (globalOpts) {
    globalOpts = globalOpts || {};
    if(globalOpts.config){ //convert sails to mssql
        globalOpts.config.server=globalOpts.config.host;
        globalOpts.config.userName=globalOpts.config.user;        
    }
    if(!globalOpts.fileField){
        console.log("requires field name for attachment ");
        return;
    }
    _.defaults(globalOpts, {
        server: 'your-server-name',
        username: '',
        password: ''
    });
    var adapter = {
        ls: function (dirname, cb) {
            return cb("dont use");
        },
        read: function (fd, cb) {
            return cb("dont use"); 
        },
        readLastVersion: function (fd, cb) {
            return cb("done use");
        },
        readVersion: function (fd, version, cb) {
            cb("dont use");
        },

        rm: function (fd, cb) {
            cb("dont use");
        },

        /**
         * A simple receiver for Skipper that writes Upstreams to
         * sql server
         *
         *
         * @param  {Object} options
         * @return {Stream.Writable}
         */
        receive: function sqlReceiver (options) {
            options = options || {};
            options = _.defaults(options, globalOpts);

            var receiver__ = Writable({
                objectMode: true
            });
            // This `_write` method is invoked each time a new file is received
            // from the Readable stream (Upstream) which is pumping filestreams
            // into this receiver.  (filename === `__newFile.filename`).
            receiver__._write = function onFile(__newFile, encoding, next) {
                var offset=0;
                receiver__.once('error', function (err, db) {
                    db.close();
                    done(err);
                });
                var sql="";
                mssql.connect(globalOpts.config, function (err) {
                    if (err) {
                        console.log(err);
                        return;
                    }
                    var request = new mssql.Request();
                    var getKeys=new mssql.Request();
                    var table=globalOpts.sqlTable.split('.');
                    var db="";
                    if(table.length>1){
                        db=table[0]+".";
                    }
                    table=table[table.length-1];
                    var sql="SELECT KU.table_name as tablename,column_name as primarykeycolumn FROM "+db+"INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC INNER JOIN "+db+"INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME and ku.table_name='"+table+"' ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;";
                    var holdKeys={};
                    var carryOutInsert=function(){
                        var sql="INSERT INTO "+globalOpts.sqlTable+" (";
                        var sqlCol=" VALUES (";
                        var output=" OUTPUT";
                        if(globalOpts.parameters){
                            for(parm in globalOpts.parameters){
                                sql+=parm+", ";
                                sqlCol+=globalOpts.parameters[parm]+", ";
                                if(holdKeys[parm.toLowerCase()]){//if primary key
                                    output+=" Inserted."+parm+', ';
                                }
                            }
                        }
                        sql+=globalOpts.fileField+') ';
                        sqlCol+="0x)";
                        if(output===' OUTPUT'){ //if no primary keys in the parameters given
                            output=';SELECT SCOPE_IDENTITY() as '+Object.keys(holdKeys)[0]; 
                            sql=sql+sqlCol+output;
                        }
                        else{
                            sql=sql+output.substring(0, output.length-2)+' '+sqlCol;
                        }
                        request.query(sql, function(err, rs, affected){
                            if(err){
                                console.log(err);
                            }
                            var data=rs[0];
                            var condition="";

                            for(key in data){
                                var val=data[key];
                                if(typeof val==='string'){
                                    val="'"+val+"'";
                                }
                                else if(val instanceof Date){
                                    val="'"+val.toISOString().slice(0, 23).replace('T', ' ')+"'";
                                }
                                condition+=key+"="+val+" AND ";
                            }
                            condition=condition.substring(0, condition.length-5);
                            var numSize=0;
                            var numQuery=0;
                            var arrayChunks={};//this holds chunks in queue
                            var sql="UPDATE "+globalOpts.sqlTable+" SET "+globalOpts.fileField+" .WRITE(@buff, NULL, NULL) WHERE "+condition;
                            var qur=function(){
                                request.input('buff', mssql.VarBinary(arrayChunks['chunk'+numQuery].length), arrayChunks['chunk'+numQuery]);//
                                request.query(sql, function(err, rs){
                                    if(err){
                                        console.log(err);
                                    }
                                    arrayChunks['chunk'+numQuery]=""; //remove ram
                                    numQuery++;
                                    if(numQuery<numSize){
                                        qur();
                                    }
                                });
                            }
                            __newFile.on('data', function(chunk){
                                arrayChunks['chunk'+numSize]=chunk;
                                if(numQuery>=numSize){
                                    qur();
                                }
                                numSize++;
                            });
                            __newFile.on('end', function(){
                                var keys=Object.keys(arrayChunks);
                                if(globalOpts.callback){
                                    globalOpts.callback(data);
                                }
                            });

                        });
                    }
                    getKeys.query(sql, function(err, rs, affected){
                        var n=rs.length;
                        for(var i=0; i<n; i++){
                            holdKeys[rs[i].primarykeycolumn.toLowerCase()]='pk';
                        }
                        carryOutInsert();
                    });
                });
                next();
            };
            return receiver__;
        }
    };
    return adapter;
    
};