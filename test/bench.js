// Hash db bench

var sys = require('sys');
var TC = require('../build/default/tokyocabinet');
var fs = require('fs');

sys.puts("Tokyo Cabinet version " + TC.VERSION);

var samples = [];
var next_sample = function () {
  var next = samples.shift();
  if (next) next();
}
setTimeout(next_sample, 10);

var put_count = 100000;

var hdb;
var hdb;

samples.push(function() {
  sys.puts('sync');

  hdb = new TC.HDB;
  if (!hdb.open('casket1.tch', TC.HDB.OWRITER | TC.HDB.OCREAT)) {
    sys.error(hdb.errmsg());
  }

  var t = Date.now();
  for (var i = 0; i < put_count; i++) {
    if (!hdb.put('key' + i, 'val' + i + ' 0123456789')) {
      sys.error(hdb.errmsg());
    }
  }
  sys.puts(Date.now() - t);
  for (var i = 0; i < 10; i++) {
    var val = hdb.get('key' + i);
    if (!val) {
      sys.error(hdb.errmsg());
    } else {
      sys.puts(val);
    }
  }

  next_sample();
});

samples.push(function() {
  sys.puts('async');

  var hdb = new TC.HDB;
  // this line is necessary for an async operation
  if (!hdb.setmutex()) throw hdb.errmsg();

  hdb.openAsync('casket2.tch', TC.HDB.OWRITER | TC.HDB.OCREAT, function(e) {
    if (e) sys.error(hdb.errmsg(e));

    var t = Date.now();
    var n = put_count;
    for (var i = 0; i < put_count; i++) {
      hdb.putAsync('key' + i, 'val' + i + ' 0123456789', function(e) {
        if (e) sys.error(hdb.errmsg(e));
        if (--n === 0) {
          sys.puts(Date.now() - t);

          n = 10;
          for (var i = 0; i < 10; i++) {
            hdb.getAsync('key' + i, function(e, val) {
              if (e) {
                sys.puts(hdb.errmsg(e));
              } else {
                sys.puts(val);
              }
              if (--n === 0) {
                next_sample();
              }
            });
          }
        }
      });
    }
  });
});


samples.push(function() {
  sys.puts('async');

  var hdb = new TC.HDB;
  // this line is necessary for an async operation
  if (!hdb.setmutex()) throw hdb.errmsg();

  hdb.openAsync('casket3.tch', TC.HDB.OWRITER | TC.HDB.OCREAT, function(e) {
    if (e) sys.error(hdb.errmsg(e));

    var t = Date.now();
    var i = 0;
    (function func() {
      hdb.putAsync('key' + i, 'val' + i + ' 0123456789', function(e) {
        if (++i < put_count) {
          func();
        } else {
          sys.puts(Date.now() - t);

          var n = 10;
          for (var j = 0; j < 10; j++) {
            hdb.getAsync('key' + j, function(e, val) {
              if (e) {
                sys.puts(hdb.errmsg(e));
              } else {
                sys.puts(val);
              }
              if (--n === 0) {
                next_sample();
              }
            });
          }
        }
      });
    }());
  });
});

