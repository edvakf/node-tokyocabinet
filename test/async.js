// sample codes at http://1978th.net/tokyocabinet/spex-en.html
// translated into JS.

var sys = require('sys');
var TC = require('../build/tokyocabinet');
var fs = require('fs');

sys.puts("Tokyo Cabinet version " + TC.VERSION);

var samples = [];
var next_sample = function () {
  var next = samples.shift();
  if (next) next();
}
setTimeout(next_sample, 10);

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

samples.push(function() {
  sys.puts("== Sample: BDB ==");
  var BDB = TC.BDB;
  var CUR = TC.BDBCUR;

  var bdb = new BDB;
  // this line is necessary for an async operation
  if (!bdb.setmutex()) throw bdb.errmsg();

  bdb.openAsync('casket.tcb', BDB.OWRITER | BDB.OCREAT, function(e) {
    if (e) sys.error(bdb.errmsg(e));

    var n = 3;
    [["foo", "hop"], ["bar", "step"], ["baz", "jump"]].forEach(function(kv) {
      bdb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error(bdb.errmsg(e));

        if (--n === 0) {
          bdb.getAsync("foo", function(e, value) {
            sys.puts(value);

            var cur = new CUR(bdb);
            cur.firstAsync(function func(e) { // recursive asynchronous function

              cur.keyAsync(function(e, key) {
                if (e !== BDB.ENOREC) {

                  cur.valAsync(function(e, val) {
                    if (e) sys.error(bdb.errmsg(e));
                    sys.puts(key + ':' + val);
                    cur.nextAsync(func);
                  });

                } else { // if next key does not exist

                  bdb.closeAsync(function(e) {
                    if (e) sys.error(bdb.errmsg(e));
                    fs.unlink('casket.tcb');

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

samples.push(function() {
  sys.puts("== Sample: FDB ==");
  var FDB = TC.FDB;

  var fdb = new FDB;
  // this line is necessary for an async operation
  if (!fdb.setmutex()) throw fdb.errmsg();

  fdb.openAsync('casket.tcf', FDB.OWRITER | FDB.OCREAT, function(e) {
    if (e) sys.error(fdb.errmsg(e));

    var n = 3;
    [["1", "one"], ["12", "twelve"], ["144", "one forty four"]].forEach(function(kv) {
      fdb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error(fdb.errmsg(e));

        if (--n === 0) {
          fdb.getAsync("1", function(e, value) {
            if (e) sys.error(fdb.errmsg(e));
            sys.puts(value);

            fdb.iterinitAsync(function(e) {
              if (e) sys.error(fdb.errmsg(e));

              fdb.iternextAsync(function func(e ,key) { // recursive asynchronous function
                if (e !== FDB.ENOREC) { // if next key exsists
                  if (e) sys.error(fdb.errmsg(e));

                  fdb.getAsync(key, function(e, value) {
                    if (e) sys.error(fdb.errmsg(e));
                    sys.puts(key + ':' + value);
                    fdb.iternextAsync(func);
                  });

                } else { // if next key does not exist

                  fdb.closeAsync(function(e) {
                    if (e) sys.error(fdb.errmsg(e));
                    fs.unlink('casket.tcf');

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

samples.push(function() {
  sys.puts("== Sample: TDB ==");
  var TDB = TC.TDB;
  var QRY = TC.TDBQRY;

  var tdb = new TDB;
  // this line is necessary for an async operation
  if (!tdb.setmutex()) throw tdb.errmsg();

  tdb.openAsync('casket.tct', TDB.OWRITER | TDB.OCREAT, function(e) {
    if (e) sys.error(tdb.errmsg(e));

    var n = 3;
    [[tdb.genuid(), {"name": "mikio", "age": "30", "lang": "ja,en,c"}], 
    [tdb.genuid(), {"name": "joker", "age": "19", "lang": "en,es"}],
    [tdb.genuid(), {"name": "falcon", "age": "31","lang": "ja"}]].forEach(function(kv) {

      tdb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error(tdb.errmsg(e));

        if (--n === 0) {
          var qry = new QRY(tdb);
          qry.addcond("age", QRY.QCNUMGE, "20");
          qry.addcond("lang", QRY.QCSTROR, "ja,en");
          qry.setorder("name", QRY.QOSTRASC);
          qry.setlimit(10, 0);
          qry.searchAsync(function(e, res) {

            n = res.length;
            res.forEach(function(r) {
              tdb.getAsync(r, function(e, cols) {
                if (e) sys.error(tdb.errmsg(e));

                if (cols) {
                  sys.print(r);
                  Object.keys(cols).forEach(function(name) {
                    sys.print("\t" + name + "\t" + cols[name])
                  });
                  sys.print("\n");
                }

                if (--n === 0) {
                  tdb.closeAsync(function(e) {
                    if (e) sys.error(tdb.errmsg(e));
                    fs.unlink('casket.tct');

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

samples.push(function() {
  sys.puts("== Sample: ADB ==");
  var ADB = TC.ADB;

  var adb = new ADB;

  // no need to manually set mutex since adb automatically does it on open
  adb.openAsync('casket.tcb', function(e) {
    if (e) sys.error(adb.errmsg(e));

    var n = 3;
    [["foo", "hop"], ["bar", "step"], ["baz", "jump"]].forEach(function(kv) {
      adb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error([e, "put error"]);

        if (--n === 0) {
          adb.getAsync("foo", function(e, value) {
            if (e) sys.error([e, 'get error']);
            sys.puts(value);

            adb.iterinitAsync(function(e) {
              if (e) sys.error([e, 'iterinit error']);

              adb.iternextAsync(function func(e ,key) { // recursive asynchronous function
                if (key !== null) { // if next key exsists

                  adb.getAsync(key, function(e, value) {
                    if (e) sys.error([e, 'get error']);
                    sys.puts(key + ':' + value);
                    adb.iternextAsync(func);
                  });

                } else { // if next key does not exist

                  adb.closeAsync(function(e) {
                    if (e) sys.error([e, 'close error']);
                    fs.unlink('casket.tcb');

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

