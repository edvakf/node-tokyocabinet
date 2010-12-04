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

var syncdb;
var asyncdb;

samples.push(function() {
  sys.puts('sync');

  syncdb = new TC.HDB;
  if (!syncdb.open('casket1.tch', TC.HDB.OWRITER | TC.HDB.OCREAT)) {
    sys.error(syncdb.errmsg());
  }

  var t = Date.now();
  for (var i = 0; i < put_count; i++) {
    if (!syncdb.put('key' + i, 'val' + i + ' 0123456789')) {
      sys.error(hdb.errmsg());
    }
  }
  sys.puts(Date.now() - t);
  for (var i = 0; i < 20; i++) {
    var val = syncdb.get('key' + i);
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

  var asyncdb = new TC.HDB;
  // this line is necessary for an async operation
  if (!asyncdb.setmutex()) throw asyncdb.errmsg();

  asyncdb.openAsync('casket2.tch', TC.HDB.OWRITER | TC.HDB.OCREAT, function(e) {
    if (e) sys.error(asyncdb.errmsg(e));

    var t = Date.now();
    var n = put_count;
    for (var i = 0; i < put_count; i++) {
      asyncdb.putAsync('key' + i, 'val' + i + ' 0123456789', function(e) {
        if (e) sys.error(asyncdb.errmsg(e));
        if (--n === 0) {
          sys.puts(Date.now() - t);

          for (var i = 0; i < 20; i++) {
            asyncdb.getAsync('key' + i, function(e, val) {
              if (e) {
                sys.puts(hdb.errmsg(e));
              } else {
                sys.puts(val);
              }
            });
          }
        }
      });
    }
  });
});

