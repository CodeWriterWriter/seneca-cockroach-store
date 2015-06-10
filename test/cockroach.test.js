/* Copyright (c) 2010-2014 Richard Rodger, MIT License */

"use strict";


var seneca = require('seneca')

var shared = require('seneca-store-test')

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

var si = seneca({log:'silent'})
si.use('../cockroach-store.js')

si.__testcount = 0
var testcount = 0


describe('cockroach', function(){
  it('basic', function(done){
    testcount++
    shared.basictest(si,done)
  })

  it('close', function(done){
    shared.closetest(si,testcount,done)
  })
})
