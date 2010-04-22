#include <v8.h>
#include <node.h>
#include <tcutil.h>
#include <tchdb.h>
#include <tcbdb.h>
#include <tcfdb.h>
#include <tctdb.h>
#include <tcadb.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

#define THROW_BAD_ARGS \
  ThrowException(Exception::TypeError(String::New("Bad arguments")))

#define DEFINE_PREFIXED_CONSTANT(target, prefix, constant) \
  (target)->Set(String::NewSymbol(#constant), \
                Integer::New(prefix##constant))

#define THIS args.This()

#define ARG0 args[0]
#define ARG1 args[1]
#define ARG2 args[2]
#define ARG3 args[3]
#define ARG4 args[4]
#define ARG5 args[5]
#define ARG6 args[6]
#define ARG7 args[7]

#define VDOUBLE(obj) ((obj)->NumberValue())
#define VINT32(obj) ((obj)->Int32Value())
#define VINT64(obj) ((obj)->IntegerValue())
#define VSTRPTR(obj) (*String::Utf8Value((obj)->ToString()))
#define VSTRSIZ(obj) ((obj)->ToString()->Utf8Length())
#define VBOOL(obj) ((obj)->BooleanValue())
// null or undefined
#define NOU(obj) ((obj)->IsNull() || (obj)->IsUndefined())

using namespace v8;
using namespace node;

inline TCLIST* arytotclist (const Handle<Array> ary) {
  HandleScope scope;
  int len = ary->Length();
  TCLIST *list = tclistnew2(len);
  for (int i = 0; i < len; i++) {
    tclistpush2(list, VSTRPTR(ary->Get(Integer::New(i))));
  }
  return list;
}

inline Local<Array> tclisttoary (TCLIST *list) {
  HandleScope scope;
  int len = tclistnum(list);
  Local<Array> ary = Array::New(len);
  for (int i = 0; i < len; i++) {
    ary->Set(Integer::New(i), String::New(tclistval2(list, i)));
  }
  return ary;
}

inline TCMAP* objtotcmap (const Handle<Object> obj) {
  HandleScope scope;
  TCMAP *map = tcmapnew2(31);
  Local<Array> keys = obj->GetPropertyNames();
  int len = keys->Length();
  Local<Value> key, val;
  for (int i = 0; i < len; i++) {
    key = keys->Get(Integer::New(i));
    val = obj->Get(key);
    if (NOU(val)) continue;
    tcmapput(map, VSTRPTR(key), VSTRSIZ(key), VSTRPTR(val), VSTRSIZ(val));
  }
  return map;
}

inline Local<Object> tcmaptoobj (TCMAP *map) {
  const char *kbuf, *vbuf;
  int ksiz, vsiz;
  HandleScope scope;
  Local<Object> obj = Object::New();
  tcmapiterinit(map);
  for (;;) {
    kbuf = static_cast<const char*>(tcmapiternext(map, &ksiz));
    if (kbuf == NULL) break;
    vbuf = static_cast<const char*>(tcmapiterval(kbuf, &vsiz));
    obj->Set(String::New(kbuf, ksiz), String::New(vbuf, vsiz));
  }
  return obj;
}

inline void set_ecodes (const Handle<FunctionTemplate> tmpl) {
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ESUCCESS);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ETHREAD);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EINVALID);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ENOFILE);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ENOPERM);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EMETA);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ERHEAD);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EOPEN);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ECLOSE);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ETRUNC);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ESYNC);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ESTAT);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ESEEK);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EREAD);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EWRITE);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EMMAP);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ELOCK);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EUNLINK); 
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ERENAME); 
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EMKDIR);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ERMDIR);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EKEEP);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, ENOREC);
  DEFINE_PREFIXED_CONSTANT(tmpl, TC, EMISC);
}

class HDB : ObjectWrap {
  public:
    HDB () : ObjectWrap () {
      db = tchdbnew();
    }

    ~HDB () {
      tchdbdel(db);
    }

    static TCHDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<HDB>(obj)->db;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);
      tmpl->InstanceTemplate()->SetInternalFieldCount(1);
      set_ecodes(tmpl);

      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, TLARGE);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, TDEFLATE);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, TBZIP);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, TTCBS);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OREADER);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OWRITER);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OCREAT);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OTRUNC);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, ONOLCK);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OLCKNB);
      DEFINE_PREFIXED_CONSTANT(tmpl, HDB, OTSYNC);

      NODE_SET_PROTOTYPE_METHOD(tmpl, "errmsg", Errmsg);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "ecode", Ecode);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tune", Tune);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setcache", Setcache);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setxmsiz", Setxmsiz);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setdfunit", Setdfunit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putasync", Putasync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", Iterinit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", Iternext);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeys", Fwmkeys);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fsiz", Fsiz);

      target->Set(String::New("HDB"), tmpl->GetFunction());
    }

  private:
    TCHDB *db;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      (new HDB)->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Errmsg (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tchdberrmsg(
          NOU(ARG0) ? tchdbecode(Unwrap(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tchdbecode(Unwrap(THIS));
      return Integer::New(ecode);
    }

    static Handle<Value>
    Tune (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3))) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbtune(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? -1 : VINT32(ARG3));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setcache (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetcache(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setxmsiz (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetxmsiz(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setdfunit (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetdfunit(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbopen(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? HDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbclose(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbput(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputkeep(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputcat(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putasync (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputasync(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbout(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      char *vstr = static_cast<char *>(tchdbget(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tchdbvsiz(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbiterinit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      char *vstr = tchdbiternext2(
          Unwrap(THIS));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Fwmkeys (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *fwmkeys = tchdbfwmkeys(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      Local<Array> ary = tclisttoary(fwmkeys);
      tclistdel(fwmkeys);
      return ary;
    }

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      int sum = tchdbaddint(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      double sum = tchdbadddouble(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbsync(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3))) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdboptimize(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? UINT8_MAX : VINT32(ARG3));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbvanish(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int success = tchdbcopy(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtranbegin(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtrancommit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtranabort(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tchdbpath(
          Unwrap(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tchdbrnum(
          Unwrap(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tchdbfsiz(
          Unwrap(THIS));
      return Integer::New(siz);
    }
};

class BDB : ObjectWrap {
  public:
    const static Persistent<FunctionTemplate> Tmpl;

    BDB () : ObjectWrap () {
      db = tcbdbnew();
    }

    ~BDB () {
      tcbdbdel(db);
    }

    static TCBDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<BDB>(obj)->db;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      set_ecodes(Tmpl);
      Tmpl->InstanceTemplate()->SetInternalFieldCount(1);

      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, TLARGE);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, TDEFLATE);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, TBZIP);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, TTCBS);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OREADER);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OWRITER);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OCREAT);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OTRUNC);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, ONOLCK);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OLCKNB);
      DEFINE_PREFIXED_CONSTANT(Tmpl, BDB, OTSYNC);

      NODE_SET_PROTOTYPE_METHOD(Tmpl, "errmsg", Errmsg);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "ecode", Ecode);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tune", Tune);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setcache", Setcache);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setxmsiz", Setxmsiz);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setdfunit", Setdfunit);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putdup", Putdup);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putlist", Putlist);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "outlist", Outlist);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "getlist", Getlist);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vnum", Vnum);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "range", Range);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeys", Fwmkeys);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fsiz", Fsiz);

      target->Set(String::New("BDB"), Tmpl->GetFunction());
    }

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      (new BDB)->Wrap(THIS);
      return THIS;
    }

  private:
    TCBDB *db;

    static Handle<Value>
    Errmsg (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tchdberrmsg(
          NOU(ARG0) ? tcbdbecode(Unwrap(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tcbdbecode(
          Unwrap(THIS));
      return Integer::New(ecode);
    }

    static Handle<Value>
    Tune (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3)) ||
          !(ARG4->IsNumber() || NOU(ARG4)) ||
          !(ARG5->IsNumber() || NOU(ARG5))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbtune(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT64(ARG2),
          NOU(ARG3) ? -1 : VINT32(ARG3),
          NOU(ARG4) ? -1 : VINT32(ARG4),
          NOU(ARG5) ? 0 : VINT32(ARG5));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setcache (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbsetcache(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setxmsiz (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbsetxmsiz(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setdfunit (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbsetdfunit(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbopen(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? BDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbclose(Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbput(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbputkeep(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbputcat(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putdup (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbputdup(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putlist (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !(ARG1->IsArray())) {
        return THROW_BAD_ARGS;
      }
      TCLIST *list = arytotclist(Local<Array>::Cast(ARG1));
      bool success = tcbdbputdup3(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          list);
      tclistdel(list);
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbout(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Outlist (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbout3(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      const char *vstr = static_cast<const char *>(tcbdbget3(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        return ret;
      }
    }

    static Handle<Value>
    Getlist (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      TCLIST *list = tcbdbget4(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      if (list == NULL) {
        return Null();
      } else {
        Local<Array> ary = tclisttoary(list);
        tclistdel(list);
        return ary;
      }
    }

    static Handle<Value>
    Vnum (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int num = tcbdbvnum(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(num);
    }

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tcbdbvsiz(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Range (const Arguments& args) {
      HandleScope scope;
      TCLIST *list = tcbdbrange(
          Unwrap(THIS),
          NOU(ARG0) ? NULL : VSTRPTR(ARG0),
          NOU(ARG0) ? -1 : VSTRSIZ(ARG0),
          NOU(ARG1) ? false : VBOOL(ARG1),
          NOU(ARG2) ? NULL : VSTRPTR(ARG2),
          NOU(ARG2) ? -1 : VSTRSIZ(ARG2),
          NOU(ARG3) ? false : VBOOL(ARG3),
          NOU(ARG4) ? -1 : VINT32(ARG4));
      Local<Array> arr = tclisttoary(list);
      tclistdel(list);
      return arr;
    }

    static Handle<Value>
    Fwmkeys (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *fwmkeys = tcbdbfwmkeys(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      Local<Array> ary = tclisttoary(fwmkeys);
      tclistdel(fwmkeys);
      return ary;
    }

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      int sum = tcbdbaddint(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      double sum = tcbdbadddouble(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbsync(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3)) ||
          !(ARG4->IsNumber() || NOU(ARG4)) ||
          !(ARG5->IsNumber() || NOU(ARG5))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbtune(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT64(ARG2),
          NOU(ARG3) ? -1 : VINT32(ARG3),
          NOU(ARG4) ? -1 : VINT32(ARG4),
          NOU(ARG5) ? UINT8_MAX : VINT32(ARG5));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbvanish(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int success = tcbdbcopy(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtranbegin(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtrancommit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtranabort(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcbdbpath(
          Unwrap(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcbdbrnum(
          Unwrap(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcbdbfsiz(
          Unwrap(THIS));
      return Integer::New(siz);
    }
};

const Persistent<FunctionTemplate> BDB::Tmpl =
  Persistent<FunctionTemplate>::New(FunctionTemplate::New(BDB::New));

class CUR : ObjectWrap {
  public:
    CUR (TCBDB *bdb) : ObjectWrap () {
      cur = tcbdbcurnew(bdb);
    }

    ~CUR () {
      tcbdbcurdel(cur);
    }

    static BDBCUR *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<CUR>(obj)->cur;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);

      Local<ObjectTemplate> ot = tmpl->InstanceTemplate();
      ot->SetInternalFieldCount(1);

      DEFINE_PREFIXED_CONSTANT(tmpl, BDB, CPCURRENT);
      DEFINE_PREFIXED_CONSTANT(tmpl, BDB, CPBEFORE);
      DEFINE_PREFIXED_CONSTANT(tmpl, BDB, CPAFTER);

      NODE_SET_PROTOTYPE_METHOD(tmpl, "first", First);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "last", Last);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "jump", Jump);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "prev", Prev);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "next", Next);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "key", Key);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "val", Val);

      target->Set(String::New("BDBCUR"), tmpl->GetFunction());
    }

  private:
    BDBCUR *cur;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !BDB::Tmpl->HasInstance(ARG0)) {
        return THROW_BAD_ARGS;
      }
      TCBDB *db = BDB::Unwrap(Local<Object>::Cast(ARG0));
      (new CUR(db))->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    First (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurfirst(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Last (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurlast(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Jump (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbcurjump(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Prev (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurprev(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Next (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurnext(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG2->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbcurput(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? BDBCPCURRENT : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurout(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Key (const Arguments& args) {
      HandleScope scope;
      int ksiz;
      char *key = static_cast<char *>(tcbdbcurkey(
          Unwrap(THIS),
          &ksiz));
      if (key == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(key, ksiz);
        tcfree(key);
        return ret;
      }
    }

    static Handle<Value>
    Val (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *val = static_cast<char *>(tcbdbcurval(
          Unwrap(THIS),
          &vsiz));
      if (val == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(val, vsiz);
        tcfree(val);
        return ret;
      }
    }
};

class FDB : ObjectWrap {
  public:
    FDB () : ObjectWrap () {
      db = tcfdbnew();
    }

    ~FDB () {
      tcfdbdel(db);
    }

    static TCFDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<FDB>(obj)->db;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);
      set_ecodes(tmpl);
      tmpl->InstanceTemplate()->SetInternalFieldCount(1);

      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OREADER);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OWRITER);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OCREAT);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OTRUNC);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, ONOLCK);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OLCKNB);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OTSYNC);

      NODE_SET_PROTOTYPE_METHOD(tmpl, "errmsg", Errmsg);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "ecode", Ecode);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tune", Tune);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", Iterinit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", Iternext);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "range", Range);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fsiz", Fsiz);

      target->Set(String::New("FDB"), tmpl->GetFunction());
    }

  private:
    TCFDB *db;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      (new FDB)->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Errmsg (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tcfdberrmsg(
          NOU(ARG0) ? tcfdbecode(Unwrap(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tcfdbecode(
          Unwrap(THIS));
      return Integer::New(ecode);
    }

    static Handle<Value>
    Tune (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbtune(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT64(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbopen(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? FDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbclose(Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbput2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbputkeep2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbputcat2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbout2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      char *vstr = static_cast<char *>(tcfdbget2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tcfdbvsiz2(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbiterinit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *vstr = static_cast<char *>(tcfdbiternext2(
          Unwrap(THIS),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Range (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 || 
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *list = tcfdbrange4(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG2) ? -1 : VINT32(ARG2));
      Local<Array> arr = tclisttoary(list);
      tclistdel(list);
      return arr;
    }

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      int sum = tcfdbaddint(
          Unwrap(THIS),
          tcfdbkeytoid(VSTRPTR(ARG0), VSTRSIZ(ARG0)),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      double sum = tcfdbadddouble(
          Unwrap(THIS),
          tcfdbkeytoid(VSTRPTR(ARG0), VSTRSIZ(ARG0)),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbsync(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdboptimize(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT64(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbvanish(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int success = tcfdbcopy(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtranbegin(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtrancommit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtranabort(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcfdbpath(
          Unwrap(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcfdbrnum(
          Unwrap(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcfdbfsiz(
          Unwrap(THIS));
      return Integer::New(siz);
    }
};

class TDB : ObjectWrap {
  public:
    const static Persistent<FunctionTemplate> Tmpl;

    TDB () : ObjectWrap () {
      db = tctdbnew();
    }

    ~TDB () {
      tctdbdel(db);
    }

    static TCTDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<TDB>(obj)->db;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      set_ecodes(Tmpl);
      Tmpl->InstanceTemplate()->SetInternalFieldCount(1);

      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, TLARGE);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, TDEFLATE);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, TBZIP);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, TTCBS);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OREADER);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OWRITER);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OCREAT);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OTRUNC);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ONOLCK);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OLCKNB);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, OTSYNC);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITLEXICAL);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITDECIMAL);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITTOKEN);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITQGRAM);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITOPT);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITVOID);
      DEFINE_PREFIXED_CONSTANT(Tmpl, TDB, ITKEEP);

      NODE_SET_PROTOTYPE_METHOD(Tmpl, "errmsg", Errmsg);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "ecode", Ecode);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tune", Tune);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setcache", Setcache);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setxmsiz", Setxmsiz);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setdfunit", Setdfunit);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iterinit", Iterinit);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iternext", Iternext);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeys", Fwmkeys);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fsiz", Fsiz);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setindex", Setindex);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "genuid", Genuid);

      target->Set(String::New("TDB"), Tmpl->GetFunction());
    }

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      (new TDB)->Wrap(THIS);
      return THIS;
    }

  private:
    TCTDB *db;

    static Handle<Value>
    Errmsg (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tctdberrmsg(
          NOU(ARG0) ? tctdbecode(Unwrap(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tctdbecode(
          Unwrap(THIS));
      return Integer::New(ecode);
    }

    static Handle<Value>
    Tune (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3))) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbtune(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? 0 : VINT32(ARG3));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setcache (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3))) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbsetcache(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setxmsiz (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbsetxmsiz(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setdfunit (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbsetdfunit(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbopen(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? TDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbclose(Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsObject()) {
        return THROW_BAD_ARGS;
      }
      TCMAP *map = objtotcmap(Handle<Object>::Cast(ARG1));
      bool success = tctdbput(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          map);
      tcmapdel(map);
      return Boolean::New(success);
    }

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsObject()) {
        return THROW_BAD_ARGS;
      }
      TCMAP *map = objtotcmap(Handle<Object>::Cast(ARG1));
      bool success = tctdbputkeep(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          map);
      tcmapdel(map);
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsObject()) {
        return THROW_BAD_ARGS;
      }
      TCMAP *map = objtotcmap(Handle<Object>::Cast(ARG1));
      bool success = tctdbputcat(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          map);
      tcmapdel(map);
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbout(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      TCMAP *map = tctdbget(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      if (map == NULL) {
        return Null();
      } else {
        Local<Object> obj = tcmaptoobj(map);
        tcfree(map);
        return obj;
      }
    }

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tctdbvsiz(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbiterinit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *vstr = static_cast<char *>(tctdbiternext(
          Unwrap(THIS),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Fwmkeys (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *fwmkeys = tctdbfwmkeys(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      Local<Array> ary = tclisttoary(fwmkeys);
      tclistdel(fwmkeys);
      return ary;
    }

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      int sum = tctdbaddint(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      double sum = tctdbadddouble(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbsync(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1)) ||
          !(ARG2->IsNumber() || NOU(ARG2)) ||
          !(ARG3->IsNumber() || NOU(ARG3))) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdboptimize(
          Unwrap(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? UINT8_MAX : VINT32(ARG3));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbvanish(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int success = tctdbcopy(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtranbegin(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtrancommit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtranabort(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tctdbpath(
          Unwrap(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tctdbrnum(
          Unwrap(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tctdbfsiz(
          Unwrap(THIS));
      return Integer::New(siz);
    }

    static Handle<Value>
    Setindex (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      bool success = tctdbsetindex(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Genuid (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tctdbgenuid(
          Unwrap(THIS));
      return Integer::New(siz);
    }
};

const Persistent<FunctionTemplate> TDB::Tmpl =
  Persistent<FunctionTemplate>::New(FunctionTemplate::New(TDB::New));

class QRY : ObjectWrap {
  public:
    QRY (TCTDB *db) : ObjectWrap () {
      qry = tctdbqrynew(db);
    }

    ~QRY () {
      tctdbqrydel(qry);
    }

    static TDBQRY *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<QRY>(obj)->qry;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);
      set_ecodes(tmpl);

      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTREQ);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTRINC);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTRBW);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTREW);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTRAND);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTROR);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTROREQ);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCSTRRX);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMEQ);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMGT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMGE);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMLT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMLE);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMBT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNUMOREQ);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCFTSPH);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCFTSAND);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCFTSOR);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCFTSEX);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNEGATE);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QCNOIDX);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QOSTRASC);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QOSTRDESC);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QONUMASC);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QONUMDESC);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QPPUT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QPOUT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, QPSTOP);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, MSUNION);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, MSISECT);
      DEFINE_PREFIXED_CONSTANT(tmpl, TDB, MSDIFF);
      
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addcond", Addcond);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setorder", Setorder);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setlimit", Setlimit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "search", Search);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "searchout", Searchout);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "hint", Hint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "metasearch", Metasearch);

      Local<ObjectTemplate> ot = tmpl->InstanceTemplate();
      ot->SetInternalFieldCount(1);

      target->Set(String::New("TDBQRY"), tmpl->GetFunction());
    }

  private:
    TDBQRY *qry;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !TDB::Tmpl->HasInstance(ARG0)) {
        return THROW_BAD_ARGS;
      }
      TCTDB *db = TDB::Unwrap(Local<Object>::Cast(ARG0));
      (new QRY(db))->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Addcond (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 3 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      tctdbqryaddcond(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VINT32(ARG1),
          VSTRPTR(ARG2));
      return Undefined();
    }

    static Handle<Value>
    Setorder (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      tctdbqrysetorder(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? TDBQOSTRASC : VINT32(ARG1));
      return Undefined();
    }

    static Handle<Value>
    Setlimit (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0)) ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      tctdbqrysetlimit(
          Unwrap(THIS),
          NOU(ARG1) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      return Undefined();
    }

    static Handle<Value>
    Search (const Arguments& args) {
      HandleScope scope;
      TCLIST *res = tctdbqrysearch(
          Unwrap(THIS));
      Local<Array> ary = tclisttoary(res);
      tclistdel(res);
      return ary;
    }

    static Handle<Value>
    Searchout (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbqrysearchout(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Hint (const Arguments& args) {
      HandleScope scope;
      const char *hint = tctdbqryhint(
          Unwrap(THIS));
      return String::New(hint);
    }

    static Handle<Value>
    Metasearch (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsArray() ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      Local<Array> others = Local<Array>::Cast(ARG0);
      int num = others->Length();
      TDBQRY **qrys = static_cast<TDBQRY **>(tcmalloc(sizeof(*qrys) * (num+1)));
      int qnum = 0;
      qrys[qnum++] = Unwrap(THIS);
      Local<Value> oqry;
      for (int i = 0; i < num; i++) {
        oqry = others->Get(Integer::New(i));
        if (TDB::Tmpl->HasInstance(oqry)) {
          qrys[qnum++] = Unwrap(Local<Object>::Cast(oqry));
        }
      }
      TCLIST* res = tctdbmetasearch(qrys, qnum, 
          NOU(ARG1) ? TDBMSUNION : VINT32(ARG1));
      Local<Array> ary = tclisttoary(res);
      tcfree(qrys);
      tclistdel(res);
      return ary;
    }
};

class ADB : ObjectWrap {
  public:
    ADB () : ObjectWrap () {
      db = tcadbnew();
    }

    ~ADB () {
      tcadbdel(db);
    }

    static TCADB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<ADB>(obj)->db;
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);
      set_ecodes(tmpl);
      tmpl->InstanceTemplate()->SetInternalFieldCount(1);

      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", Iterinit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", Iternext);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeys", Fwmkeys);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "size", Size);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "misc", Misc);

      target->Set(String::New("ADB"), tmpl->GetFunction());
    }

  private:
    TCADB *db;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      (new ADB)->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbopen(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbclose(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbput(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbputkeep(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbputcat(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbout(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      char *vstr = static_cast<char *>(tcadbget(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tcadbvsiz(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbiterinit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      char *vstr = tcadbiternext2(
          Unwrap(THIS));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr);
        tcfree(vstr);
        return ret;
      }
    }

    static Handle<Value>
    Fwmkeys (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *fwmkeys = tcadbfwmkeys(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      Local<Array> ary = tclisttoary(fwmkeys);
      tclistdel(fwmkeys);
      return ary;
    }

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      int sum = tcadbaddint(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      double sum = tcadbadddouble(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbsync(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadboptimize(
          Unwrap(THIS),
          NOU(ARG0) ? NULL : VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbvanish(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int success = tcadbcopy(
          Unwrap(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtranbegin(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtrancommit(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtranabort(
          Unwrap(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcadbpath(
          Unwrap(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcadbrnum(
          Unwrap(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Size (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcadbsize(
          Unwrap(THIS));
      return Integer::New(siz);
    }

    static Handle<Value>
    Misc (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString() ||
          !(ARG1->IsArray() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      TCLIST *targs = NOU(ARG1) ? tclistnew2(1) 
        : arytotclist(Local<Array>::Cast(ARG1));
      TCLIST *res = tcadbmisc(
          Unwrap(THIS),
          VSTRPTR(ARG0),
          targs);
      tclistdel(targs);
      if (res == NULL) {
        return Null();
      } else {
        Local<Array> ary = tclisttoary(res);
        tclistdel(res);
        return ary;
      }
    }
};

extern "C" void
init (Handle<Object> target) {
  HandleScope scope;
  HDB::Initialize(target);
  BDB::Initialize(target);
  CUR::Initialize(target);
  FDB::Initialize(target);
  TDB::Initialize(target);
  QRY::Initialize(target);
  ADB::Initialize(target);
}

