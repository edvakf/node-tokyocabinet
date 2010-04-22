// sample codes at http://1978th.net/tokyocabinet/spex-en.html
// translated into JS.

var sys = require('sys');
var TC = require('../build/default/tokyocabinet');

(function() {
  sys.puts("== Sample: HDB ==");
  var HDB = TC.HDB;

  var hdb = new HDB;

  if (!hdb.open('casket.tch', HDB.OWRITER | HDB.OCREAT)) {
    sys.error(hdb.errmsg());
  }

  if (!hdb.put("foo", "hop") ||
      !hdb.put("bar", "step") ||
      !hdb.put("baz", "jump")) {
    sys.error(hdb.errmsg());
  }

  var value = hdb.get("foo");
  if (value) {
    sys.puts(value);
  } else {
    sys.error(hdb.errmsg());
  }

  hdb.iterinit();
  var key;
  while ((key = hdb.iternext()) !== null) {
    value = hdb.get(key);
    if (value !== null) {
      sys.puts(value);
    }
  }

  if (!hdb.vanish()) {
    sys.error(hdb.errmsg());
  }

  if (!hdb.close()) {
    sys.error(hdb.errmsg());
  }
}());

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

  if (!bdb.vanish()) {
    sys.error(bdb.errmsg());
  }

  if (!bdb.close()) {
    sys.error(bdb.errmsg());
  }
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

  if (!fdb.vanish()) {
    sys.error(fdb.errmsg());
  }

  if (!fdb.close()) {
    sys.error(fdb.errmsg());
  }
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

  if (!tdb.vanish()) {
    sys.error(tdb.errmsg());
  }

  if (!tdb.close()) {
    sys.error(tdb.errmsg());
  }
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

  if (!adb.vanish()) {
    sys.error("vanish error");
  }

  if (!adb.close()) {
    sys.error("close error");
  }
}());


