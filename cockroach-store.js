/* Copyright (c) 2015 Colm Harte, MIT License */
/* jshint node:true, asi:true, eqnull:true */
"use strict";


var _ = require('lodash')
var roach = require('roachjs')


module.exports = function(options) {


  var seneca = this
  var desc
  var dbinst = null

  function error(args,err,cb) {
    if (err) {
      seneca.log.error('entity',err,{store:name})
      cb(err)
      return true;
    }
    else
      return false;
  }

  function configure(spec,cb) {

    var dbOpts = seneca.util.deepextend({
      uri:"http://localhost:8080",
    },spec.options)

    if (!dbOpts.ssl)
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

    dbinst = new roach(dbOpts)

    seneca.log.debug('init', 'db open', dbOpts)

    cb(null)
  }


  var store = {

    name: 'cockroach-store',

    save: function(args,cb){

      var ent = args.ent

      var canon = ent.canon$({object: true})

      var update = !!ent.id

      if (!update) {
        ent.id = void 0 != ent.id$ ? ent.id$ : -1;
        delete(ent.id$)

        if (ent.id === -1) {
          this.act({role:'util', cmd:'generate_id',
                    name:canon.name, base:canon.base, zone:canon.zone, length: 10 },
                    function(err,id){
                        if (err) return cb(err);

                        ent.id = id

                        completeSave(id)
                    }
          )
        }
      }
      else
        completeSave(ent.id);


      function completeSave(id) {
        var keyId = makeKeyId(ent, id)

        dbinst.put(keyId, JSON.stringify(ent.data$(false)), function(err, res) {
          if (!error(args,err,cb)) {
            seneca.log.debug('save/update',ent,desc)
            cb(null,ent)
          }

        })
      }
    },

    load: function(args, cb) {
      var qent = args.qent
      var q = args.q

      var qq = fixquery(qent, q)


      if( qq.id ) {

        var keyId = makeKeyId(qent, qq.id)
        dbinst.get(keyId, function(err, value, res) {
          if (!error(args,err,cb)) {
            var loadedEnt = null;

            if (value) {
              var entity = JSON.parse(value)

              loadedEnt = qent.make$(entity);

            }

            seneca.log.debug('load', q, loadedEnt, desc)
            cb(null,loadedEnt);
          }

        })
      }
      else {
        store.list(args, function(err, list){
          if (list.length > 0) {
            cb(err, list[0])
          }
          else {
            cb(err, null)
          }
        })
      }

    },

    list: function(args,cb){
      var qent = args.qent
      var q    = args.q

      var qq = fixquery(qent,q)

      var startKey = makeKeyId(qent, new Array(11).join("0"))
      var endKey = makeKeyId(qent, new Array(11).join("z"))

      var list = []

      dbinst.scan(startKey, endKey, 0, function(err, rows, result){
        if (!error(args, err, cb))
        {
          for(var i = 0; i < rows.length; i ++) {

            var rowItem = rows[i].value.bytes.toBuffer() || null;

            if (rowItem) {
              rowItem = JSON.parse(rowItem)
            }

            if (rowItem && isValidRow(qq, rowItem)) {
              list.push(qent.make$(rowItem))
            }

          }

          // sort first
          if (q.sort$) {
            for (var sf in q.sort$) break;
            var sd = qq.sort$[sf] < 0 ? -1 : 1

            list = list.sort(function(a,b){
              return sd * ( a[sf] < b[sf] ? -1 : a[sf] === b[sf] ? 0 : 1 )
            })
          }

          if (q.skip$) {
            list = list.slice(qq.skip$)
          }

          if (q.limit$) {
            list = list.slice(0,qq.limit$)
          }

          seneca.log.debug('list',q,list.length,list[0],desc)
          cb(null,list)

        }
      })

    },


    remove: function(args,cb){
      var qent = args.qent
      var q    = args.q

      var all  = q.all$ // default false
      var load  = _.isUndefined(q.load$) ? true : q.load$ // default true

      var qq = fixquery(qent, q)

      if (all) {
        var startKey = makeKeyId(qent, new Array(11).join("0"))
        var endKey = makeKeyId(qent, new Array(11).join("z"))

        dbinst.deleteRange(startKey, endKey, 0, function(err, deleted, result){
          if (!error(args, err, cb))
          {
            seneca.log.debug('remove/all',q,desc)
            cb(err)
          }
        })
      }
      else {

        store.load(args, function(err, data){

            if (!error(args, err, cb))
            {

              if (data && data.id) {

                var keyId = makeKeyId(qent, data.id)

                dbinst.delete(keyId, function(err, result){
                  if (!error(args, err, cb))
                  {
                    seneca.log.debug('remove/one', q, data, desc)

                    console.log("loaded is " + load)
                    var ent = load ? data : null
                    cb(err,ent)
                  }
                })
              }
            }

          })
      }

    },


    close: function(args,cb){
      this.log.debug('close',desc)
      cb()
    },


    native: function(args,cb){
      cb(null,dbinst)
    }
  }



  var meta = this.store.init(this,options,store)

  desc = meta.desc

  options.idlen = options.idlen || 10

  this.add({role:store.name,cmd:'dump'},function(args,cb){
    cb(null,entmap)
  })

  this.add({role:store.name,cmd:'export'},function(args,done){
    var entjson = JSON.stringify(entmap)
    fs.writeFile(args.file,entjson,function(err){
      done(err,{ok:!!err})
    })
  })


  this.add({role:store.name,cmd:'import'},function(args,done){
    try {
      fs.readFile(args.file,function(err,entjson){
        if( entjson ) {
          try {
            entmap = JSON.parse(entjson)
            done(err,{ok:!!err})
          }
          catch(e){
            done(e)
          }
        }
      })
    }
    catch(e){
      done(e)
    }
  })

  var meta = seneca.store.init(seneca,options,store)
  desc = meta.desc

  seneca.add({init:store.name,tag:meta.tag},function(args,done){
    configure(options,function(err){
      if (err) return seneca.die('store',err,{store:store.name,desc:desc});
      return done();
    })
  })


  return {name:store.name,tag:meta.tag}
}


function makeKeyId(ent, id) {
  var canon = ent.canon$({object: true})

  return (canon.base ? canon.base + '_' : '') + canon.name + '_' + id

}

function isValidRow(q, data)
{

  for(var p in q) {

    if( !~p.indexOf('$') && q[p] != data[p] ) {
      return false
    }
  }

  return true

}

function fixquery(qent, q) {
  return null==q ? {} : _.isString(q) ? {id: q} : _.isString(q.id) ? q : q
}
