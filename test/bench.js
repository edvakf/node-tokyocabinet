// Hash db bench

var sys = require('sys');
var TC = require('../build/default/tokyocabinet');
var fs = require('fs');

sys.puts("Tokyo Cabinet version " + TC.VERSION);

/*
var samples = [];
var next_sample = function () {
  var next = samples.shift();
  if (next) next();
}
setTimeout(next_sample, 10);

var put_count = 100000;

var syncdb;
var asyncdb;
*/

//samples.push(function() {
  syncdb = new TC.HDB;
  if (!syncdb.open('casket.tch', TC.HDB.OWRITER | TC.HDB.OCREAT)) {
    sys.error(syncdb.errmsg());
  }
  syncdb.put('a','b');
  /*
  var t = Date.now();
  for (var i = 0; i < put_count; i++) {
    syncdb.put('key' + i, '0123456789');
  }
  sys.puts(Date.now() - t);
  */
//});

/*
samples.push(function() {
  sys.puts("== Sample: HDB ==");
  var HDB = TC.HDB;

  var hdb = new HDB;
  // this line is necessary for an async operation
  if (!hdb.setmutex()) throw hdb.errmsg();

  hdb.openAsync('casket.tch', HDB.OWRITER | HDB.OCREAT, function(e) {
    if (e) sys.error(hdb.errmsg(e));

    var n = 3;
    [["foo", "hop"], ["bar", "step"], ["baz", "jump"]].forEach(function(kv) {
      hdb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error(hdb.errmsg(e));

        if (--n === 0) {
          hdb.getAsync("foo", function(e, value) {
            if (e) sys.error(hdb.errmsg(e));
            sys.puts(value);

            hdb.iterinitAsync(function(e) {
              if (e) sys.error(hdb.errmsg(e));

              hdb.iternextAsync(function func(e ,key) { // recursive asynchronous function
                if (e !== HDB.ENOREC) { // if next key exsists
                  if (e) sys.error(hdb.errmsg(e));

                  hdb.getAsync(key, function(e, value) {
                    if (e) sys.error(hdb.errmsg(e));
                    sys.puts(key + ':' + value);
                    hdb.iternextAsync(func);
                  });

                } else { // if next key does not exist

                  hdb.closeAsync(function(e) {
                    if (e) sys.error(hdb.errmsg(e));
                    fs.unlink('casket.tch');

                    next_sample();
                  });

                }
              });
            });
          });
        }
      });
    });
  });

});
*/
