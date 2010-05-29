// sample codes at http://1978th.net/tokyocabinet/spex-en.html
// translated into JS.

var sys = require('sys');
var TC = require('../build/default/tokyocabinet');
var fs = require('fs');

sys.puts("Tokyo Cabinet version " + TC.VERSION);

setTimeout(function() {
  sys.puts("== Sample: HDB ==");
  var HDB = TC.HDB;

  var hdb = new HDB;
  // this line is necessary for an async operation
  if (!hdb.setmutex()) throw hdb.errmsg();

  hdb.openAsync('casket.tch', HDB.OWRITER | HDB.OCREAT, function(e) {
    if (e) sys.error(hdb.errmsg(e));
    sys.puts("foo");

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
                  });

                }
              });
            });
          });
        }
      });
    });
  });

}, 0);

setTimeout(function() {
  sys.puts("== Sample: BDB ==");
  var BDB = TC.BDB;
  var CUR = TC.BDBCUR;

  var bdb = new BDB;
  // this line is necessary for an async operation
  if (!bdb.setmutex()) throw bdb.errmsg();

  bdb.openAsync('casket.tcb', BDB.OWRITER | BDB.OCREAT, function(e) {
    if (e) sys.error(bdb.errmsg(e));
    sys.puts("foo");

    var n = 3;
    [["foo", "hop"], ["bar", "step"], ["baz", "jump"]].forEach(function(kv) {
      bdb.putAsync(kv[0], kv[1], function(e) {
        if (e) sys.error(bdb.errmsg(e));

        if (--n === 0) {
          bdb.getAsync("foo", function(e, value) {
            if (e) sys.error(bdb.errmsg(e));
            sys.puts(value);

            var cur = new CUR(bdb);
            cur.firstAsync(function func(e) { // recursive asynchronous function

              cur.keyAsync(function(e, key) {
                if (key !== null) {

                  cur.valAsync(function(e, val) {
                    if (e) sys.error(bdb.errmsg(e));
                    sys.puts(key + ':' + val);
                    cur.nextAsync(func);
                  });

                } else { // if next key does not exist

                  bdb.closeAsync(function(e) {
                    if (e) sys.error(bdb.errmsg(e));
                    fs.unlink('casket.tcb');
                  });

                }
              });
            });
          });
        }
      });
    });
  });

}, 200);

/*
(function() {
  sys.puts("== Sample: BDB ==");

  var BDB = TC.BDB;
  var CUR = TC.BDBCUR;

  var bdb = new BDB;

  if (!bdb.open('casket.tcb', BDB.OWRITER | BDB.OCREAT)) {
    sys.error(bdb.errmsg());
  }

  if (!bdb.put("foo", "hop") ||
      !bdb.put("bar", "step") ||
      !bdb.put("baz", "jump")) {
    sys.error(bdb.errmsg());
  }

  var value = bdb.get("foo");
  if (value) {
    sys.puts(value);
  } else {
    sys.error(bdb.errmsg());
  }

  var cur = new CUR(bdb);
  cur.first();
  var key;
  while ((key = cur.key()) !== null) {
    var value = cur.val();
    if (value) {
      sys.puts(key + ":" + value);
    }
    cur.next();
  }

  if (!bdb.close()) {
    sys.error(bdb.errmsg());
  }

  fs.unlink('casket.tcb');
}());

(function() {
  sys.puts("== Sample: FDB ==");

  var FDB = TC.FDB;

  var fdb = new FDB();

  if (!fdb.open('casket.tcf', FDB.OWRITER | FDB.OCREAT)) {
    sys.error(fdb.errmsg());
  }

  if (!fdb.put("1", "one") ||
      !fdb.put("12", "twelve") ||
      !fdb.put("144", "one forty four")) {
    sys.error(fdb.errmsg());
  }

  var value = fdb.get("1");
  if (value) {
    sys.puts(value);
  } else {
    sys.error(fdb.errmsg());
  }

  fdb.iterinit();
  var key;
  while((key = fdb.iternext()) != null) {
    var value = fdb.get(key);
    if (value) {
      sys.puts(key + ':' + value);
    }
  }

  if (!fdb.close()) {
    sys.error(fdb.errmsg());
  }

  fs.unlink('casket.tcf');
}());

(function() {
  sys.puts("== Sample: TDB ==");

  var TDB = TC.TDB;
  var QRY = TC.TDBQRY;

  var tdb = new TDB();

  if (!tdb.open('casket.tct', TDB.OWRITER | TDB.OCREAT)) {
    sys.error(tdb.errmsg());
  }

  var pk = tdb.genuid();
  var cols = {"name": "mikio", "age": "30", "lang": "ja,en,c"};
  if (!tdb.put(pk, cols)) {
    sys.error(tdb.errmsg());
  }

  var pk = tdb.genuid();
  var cols = {"name": "joker", "age": "19", "lang": "en,es"};
  if (!tdb.put(pk, cols)) {
    sys.error(tdb.errmsg());
  }

  pk = "12345";
  cols = {};
  cols.name = "falcon";
  cols.age = "31";
  cols.lang = "ja";
  if (!tdb.put(pk, cols)) {
    sys.error(tdb.errmsg());
  }

  var qry = new QRY(tdb);
  qry.addcond("age", QRY.QCNUMGE, "20");
  qry.addcond("lang", QRY.QCSTROR, "ja,en");
  qry.setorder("name", QRY.QOSTRASC);
  qry.setlimit(10, 0);
  var res = qry.search();
  res.forEach(function(r) {
    var cols = tdb.get(r);
    if (cols) {
      sys.print(r);
      Object.keys(cols).forEach(function(name) {
        sys.print("\t" + name + "\t" + cols[name])
      });
      sys.print("\n");
    }
  });

  if (!tdb.close()) {
    sys.error(tdb.errmsg());
  }

  fs.unlink('casket.tct');
}());

(function() {
  sys.puts("== Sample: ADB ==");
  
  var ADB = TC.ADB;

  var adb = new ADB();

  if (!adb.open('casket.tcb')) {
    sys.error("open error");
  }

  if (!adb.put("foo", "hop") ||
      !adb.put("bar", "step") ||
      !adb.put("baz", "jump")) {
    sys.error("put error");
  }

  var value = adb.get("foo");
  if (value) {
    sys.puts(value);
  } else {
    sys.error("get error");
  }

  adb.iterinit();
  var key;
  while((key = adb.iternext()) != null) {
    var value = adb.get(key);
    if (value) {
      sys.puts(key + ':' + value);
    }
  }

  if (!adb.close()) {
    sys.error("close error");
  }

  fs.unlink('casket.tcb');
}());
*/
