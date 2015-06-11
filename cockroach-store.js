/* Copyright (c) 2015 Colm Harte, MIT License */
/* jshint node:true, asi:true, eqnull:true */
"use strict";


var _ = require('lodash');
var roach = require('roachjs');


module.exports = function(options) {
  var KEY_LENGTH = 20;

  var seneca = this;
  var desc;
  var dbinst = null;

  var name = "cockroach-store";

  function error(args,err,cb) {
    if (err) {
      seneca.log.error('entity',err,{store:name});
      cb(err);
      return true;
    }
    else
      return false;
  }

  function configure(spec,cb) {

    var dbOpts = seneca.util.deepextend({
      uri:"http://localhost:8080",
    },spec.options);

    dbinst = new roach(dbOpts);

    seneca.log.debug('init', 'db open', dbOpts);

    cb(null);
  }

  /*
    For each unique entity type (ie base and name) an incrementkey is maintained to store the current number of items in this key range
  */
  function generateId(ent, cb)
  {
    var canon = ent.canon$({object: true});

    var incKey = (canon.base ? canon.base + '_' : '') + canon.name + '_keyrange';

    dbinst.increment(incKey, 1, function(err, newValue, res) {
      var newId = makeId(newValue);

      cb(err, newId);
    });
  }

  /*
    Ids are created in a numerically ascending format but left padded with zeros to create a uniform length id so that range comparisions work as expected
  */
  function makeId(id)
  {
    id = id + '';
    return new Array(KEY_LENGTH - id.length + 1).join("0") + id;
  }

  var store = {

    save: function(args,cb){

      var ent = args.ent;

      var canon = ent.canon$({object: true});

      var update = !!ent.id;

      if (!update) {
        ent.id = void 0 != ent.id$ ? ent.id$ : -1;

        delete(ent.id$);

        if (ent.id === -1) {
          generateId(ent, function(err, newId){
            if (err) return cb(err);

            ent.id = newId;

            completeSave(newId);
          });

        }
        else {
          completeSave(ent.id);
        }
      }
      else
        completeSave(ent.id);


      function completeSave(id) {
        var keyId = makeKeyId(ent, id);

        dbinst.put(keyId, JSON.stringify(ent.data$(false)), function(err, res) {
          if (!error(args,err,cb)) {
            seneca.log.debug('save/update',ent,desc);
            cb(null,ent);
          }

        });
      }
    },

    load: function(args, cb) {
      var qent = args.qent;
      var q = args.q;

      var qq = fixquery(qent, q);


      if( qq.id ) {

        var keyId = makeKeyId(qent, qq.id);
        dbinst.get(keyId, function(err, value, res) {
          if (!error(args,err,cb)) {
            var loadedEnt = null;

            if (value) {
              var entity = JSON.parse(value)

              loadedEnt = qent.make$(entity);

            }

            seneca.log.debug('load', q, loadedEnt, desc);
            cb(null,loadedEnt);
          }

        })
      }
      else {
        store.list(args, function(err, list){
          if (list.length > 0) {
            cb(err, list[0]);
          }
          else {
            cb(err, null);
          }
        });
      }

    },

    list: function(args,cb){
      var qent = args.qent;
      var q    = args.q;

      var qq = fixquery(qent,q);

      var startKey = makeKeyId(qent, makeId("0"));
      var endKey = makeKeyId(qent, makeId("9"));

      var list = [];

      dbinst.scan(startKey, endKey, 0, function(err, rows, result){
        if (!error(args, err, cb))
        {
          for(var i = 0; i < rows.length; i ++) {

            var rowItem = rows[i].value.bytes.toBuffer() || null;

            if (rowItem) {
              rowItem = JSON.parse(rowItem);
            }

            if (rowItem && isValidRow(qq, rowItem)) {
              list.push(qent.make$(rowItem));
            }

          }

          // sort first
          if (q.sort$) {
            for (var sf in q.sort$) break;
            var sd = qq.sort$[sf] < 0 ? -1 : 1;

            list = list.sort(function(a,b){
              return sd * ( a[sf] < b[sf] ? -1 : a[sf] === b[sf] ? 0 : 1 )
            });
          }

          if (q.skip$) {
            list = list.slice(qq.skip$);
          }

          if (q.limit$) {
            list = list.slice(0,qq.limit$);
          }

          seneca.log.debug('list',q,list.length,list[0],desc);
          cb(null,list);

        }
      });

    },


    remove: function(args,cb){
      var qent = args.qent;
      var q    = args.q;

      var all  = q.all$; // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$; // default true

      var qq = fixquery(qent, q);

      if (all) {
        var startKey = makeKeyId(qent, makeId("0"));
        var endKey = makeKeyId(qent, makeId("9"));

        dbinst.deleteRange(startKey, endKey, 0, function(err, deleted, result){
          if (!error(args, err, cb))
          {
            seneca.log.debug('remove/all',q,desc);
            cb(err);
          }
        });
      }
      else {

        store.load(args, function(err, data){

            if (!error(args, err, cb))
            {

              if (data && data.id) {

                var keyId = makeKeyId(qent, data.id);

                dbinst.delete(keyId, function(err, result){
                  if (!error(args, err, cb))
                  {
                    seneca.log.debug('remove/one', q, data, desc);

                    var ent = load ? data : null;
                    cb(err,ent);
                  }
                });
              }
            }

          });
      }

    },


    close: function(args,cb){
      this.log.debug('close',desc);
      cb();
    },


    native: function(args,cb){
      cb(null,dbinst);
    }
  }



  var meta = this.store.init(this,options,store);

  desc = meta.desc;

  options.idlen = options.idlen || KEY_LENGTH;

  this.add({role:name,cmd:'dump'},function(args,cb){
    cb(null,entmap);
  });

  this.add({role:name,cmd:'export'},function(args,done){
    var entjson = JSON.stringify(entmap);
    fs.writeFile(args.file,entjson,function(err){
      done(err,{ok:!!err});
    });
  });


  this.add({role:name,cmd:'import'},function(args,done){
    try {
      fs.readFile(args.file,function(err,entjson){
        if( entjson ) {
          try {
            entmap = JSON.parse(entjson);
            done(err,{ok:!!err});
          }
          catch(e){
            done(e);
          }
        }
      });
    }
    catch(e){
      done(e);
    }
  });

  var meta = seneca.store.init(seneca,options,store);
  desc = meta.desc;

  seneca.add({init:name,tag:meta.tag},function(args,done){
    configure(options,function(err){
      if (err) return seneca.die('store',err,{store:name,desc:desc});
      return done();
    });
  });


  return {name:name,tag:meta.tag};
}


function makeKeyId(ent, id) {
  var canon = ent.canon$({object: true});

  return (canon.base ? canon.base + '_' : '') + canon.name + '_' + id;

}

function isValidRow(q, data)
{

  for(var p in q) {

    if( !~p.indexOf('$') && q[p] != data[p] ) {
      return false;
    }
  }

  return true;

}

function fixquery(qent, q) {
  return null==q ? {} : _.isString(q) ? {id: q} : _.isString(q.id) ? q : q;
}
