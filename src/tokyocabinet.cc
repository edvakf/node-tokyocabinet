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
#include <iostream>

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

#define VDOUBLE(obj) ((obj)->NumberValue())
#define VINT32(obj) ((obj)->Int32Value())
#define VINT64(obj) ((obj)->IntegerValue())
#define VSTRPTR(obj) (*String::Utf8Value(obj))
#define VSTRSIZ(obj) (String::Utf8Value(obj).length())
#define VBOOL(obj) ((obj)->BooleanValue())
// null or undefined
#define NOU(obj) ((obj)->IsNull() || (obj)->IsUndefined())

/* async method blueprint */
#define DEFINE_ASYNC_FUNC(name)                                               \
  static Handle<Value>                                                        \
  name##Async (const Arguments& args) {                                       \
    HandleScope scope;                                                        \
    if (!name##Data::checkArgs(args)) {                                       \
      return THROW_BAD_ARGS;                                                  \
    }                                                                         \
    name##Data *data = new name##Data;                                        \
    data->init(args);                                                         \
    eio_custom(Exec##name, EIO_PRI_DEFAULT, After##name, data);               \
    ev_ref(EV_DEFAULT_UC);                                                    \
    return Undefined();                                                       \
  }                                                                           \

#define DEFINE_ASYNC_EXEC(name)                                               \
  static int                                                                  \
  Exec##name (eio_req *req) {                                                 \
    name##Data *data = static_cast<name##Data *>(req->data);                  \
    req->result = data->run() ? TCESUCCESS : data->ecode();                   \
    return 0;                                                                 \
  }                                                                           \

#define DEFINE_ASYNC_AFTER(name)                                              \
  static int                                                                  \
  After##name (eio_req *req) {                                                \
    HandleScope scope;                                                        \
    name##Data *data = static_cast<name##Data *>(req->data);                  \
    if (data->hasCallback) {                                                  \
      data->callCallback(Integer::New(req->result));                          \
    }                                                                         \
    ev_unref(EV_DEFAULT_UC);                                                  \
    delete data;                                                              \
    return 0;                                                                 \
  }

#define DEFINE_ASYNC_AFTER2(name)                                             \
  static int                                                                  \
  After##name (eio_req *req) {                                                \
    HandleScope scope;                                                        \
    name##Data *data = static_cast<name##Data *>(req->data);                  \
    if (data->hasCallback) {                                                  \
      data->callCallback(Integer::New(req->result), data->returnValue());     \
    }                                                                         \
    ev_unref(EV_DEFAULT_UC);                                                  \
    delete data;                                                              \
    return 0;                                                                 \
  }

#define DEFINE_ASYNC(name)                                                    \
  DEFINE_ASYNC_FUNC(name)                                                     \
  DEFINE_ASYNC_EXEC(name)                                                     \
  DEFINE_ASYNC_AFTER(name)                                                    \

#define DEFINE_ASYNC2(name)                                                   \
  DEFINE_ASYNC_FUNC(name)                                                     \
  DEFINE_ASYNC_EXEC(name)                                                     \
  DEFINE_ASYNC_AFTER2(name)                                                   \

using namespace v8;
using namespace node;

// conversion between Tokyo Cabinet list/map to V8 Arrya/Object and vice versa
inline TCLIST* arytotclist (const Handle<Array> ary) {
  HandleScope scope;
  int len = ary->Length();
  TCLIST *list = tclistnew2(len);
  Handle<Value> val;
  for (int i = 0; i < len; i++) {
    val = ary->Get(Integer::New(i));
    if (val->IsString()) {
      tclistpush2(list, VSTRPTR(val));
    }
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

// object that gets passed to libeio async functions
class AsyncDataCore {
  public:
    Persistent<Function> cb;
    bool hasCallback;

    void
    setCallback (Handle<Value> cb_) {
      HandleScope scope;
      if (cb_->IsFunction()) {
        hasCallback = true;
        cb = Persistent<Function>::New(Handle<Function>::Cast(cb_));
      } else {
        hasCallback = false;
      }
    }

    ~AsyncDataCore () {
      if (hasCallback) cb.Dispose();
    }

    inline void
    callback (int argc, Handle<Value> argv[]) {
      TryCatch try_catch;
      cb->Call(Context::GetCurrent()->Global(), argc, argv);
      if (try_catch.HasCaught()) {
        FatalException(try_catch);
      }
    }

    inline void
    callCallback (Handle<Value> arg0) {
      HandleScope scope;
      Handle<Value> args[1];
      args[0] = arg0;
      callback(1, args);
    }

    inline void
    callCallback (Handle<Value> arg0, Handle<Value>arg1) {
      HandleScope scope;
      Handle<Value> args[2];
      args[0] = arg0;
      args[1] = arg1;
      callback(2, args);
    }
};

// arguments data that will be multiply inherited to make actual async objects
class ArgsDataCore {
  public:
    static bool
    checkArgs (const Arguments& args) {
      return true;
    }
};

class PathDataCore : public ArgsDataCore {
  protected:
    String::Utf8Value *path;

  public:
    void
    setPath (Handle<Value> p) {
      path = new String::Utf8Value(p);
    }

    ~PathDataCore () {
      delete path;
    }

    static bool
    checkArgs (const Arguments& args) {
      return ARG0->IsString();
    }
};

class OpenDataCore : public PathDataCore {
  protected:
    int omode;

  public:
    static bool
    checkArgs (const Arguments& args) {
      return PathDataCore::checkArgs(args) &&
        (ARG1->IsUndefined() || ARG1->IsNumber());
    }
};

// virtual inheritance of KeyDataCore and ValueDataCore from ArgsDataCore
// together solves ambiguity of checkArgs method of GetDataCore
class KeyDataCore : public virtual ArgsDataCore {
  protected:
    String::Utf8Value *kbuf;
    int ksiz;

  public:
    void
    setKey (Handle<Value> key) {
      kbuf = new String::Utf8Value(key);
      ksiz = kbuf->length();
    }

    ~KeyDataCore () {
      delete kbuf;
    }

    static bool
    checkArgs (const Arguments& args) {
      return ARG0->IsString();
    }
};

class VsizDataCore : public KeyDataCore {
  protected:
    int vsiz;

  public:
    Handle<Value>
    returnValue () {
      return Number::New(vsiz);
    }
};

class PutDataCore : public KeyDataCore {
  protected:
    String::Utf8Value *vbuf;
    int vsiz;

  public:
    void
    setKeyVal (Handle<Value> key, Handle<Value> value) {
      this->setKey(key);
      vbuf = new String::Utf8Value(value);
      vsiz = vbuf->length();
    }

    ~PutDataCore () {
      delete vbuf;
    }

    static bool
    checkArgs (const Arguments& args) {
      return KeyDataCore::checkArgs(args) && ARG1->IsString();
    }
};

class ValueDataCore : public virtual ArgsDataCore {
  protected:
    char *vbuf;
    int vsiz;

  public:
    ~ValueDataCore () {
      tcfree(vbuf);
    }

    Handle<Value>
    returnValue () {
      return vbuf == NULL ? Null() : String::New(vbuf, vsiz);
    }
};

class GetDataCore : public KeyDataCore, public ValueDataCore {};

class GetListDataCore : public KeyDataCore {
  protected:
    TCLIST *list;

  public:
    ~GetListDataCore () {
      tclistdel(list);
    }

    Handle<Value>
    returnValue () {
      return tclisttoary(list);
    }
};

class FwmkeysDataCore : public GetListDataCore {
  protected:
    int max;
};

class AddintDataCore : public KeyDataCore {
  protected:
    int num;

  public:
    static bool
    checkArgs (const Arguments& args) {
      return KeyDataCore::checkArgs(args) && ARG1->IsNumber();
    }

    Handle<Value>
    returnValue () {
      return num == INT_MIN ? Null() : Integer::New(num);
    }
};

class AdddoubleDataCore : public KeyDataCore {
  protected:
    double num;

  public:
    static bool
    checkArgs (const Arguments& args) {
      return KeyDataCore::checkArgs(args) && ARG1->IsNumber();
    }

    Handle<Value>
    returnValue () {
      return isnan(num) ? Null() : Number::New(num);
    }
};


// Tokyo Cabinet
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

    static HDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<HDB>(obj);
    }

    static TCHDB *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->db;
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
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setmutex", Setmutex);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", Open);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "openAsync", OpenAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", Close);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "closeAsync", CloseAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", Put);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", Putkeep);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeepAsync", PutkeepAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", Putcat);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcatAsync", PutcatAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putasync", Putasync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putasyncAsync", PutasyncAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", Out);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", Get);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "getAsync", GetAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", Vsiz);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsizAsync", VsizAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", Iterinit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinitAsync", IterinitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", Iternext);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternextAsync", IternextAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeys", Fwmkeys);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeysAsync", FwmkeysAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", Addint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addintAsync", AddintAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", Adddouble);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddoubleAsync", AdddoubleAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", Sync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "syncAsync", SyncAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", Optimize);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimizeAsync", OptimizeAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", Vanish);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanishAsync", VanishAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", Copy);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copyAsync", CopyAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", Tranbegin);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbeginAsync", TranbeginAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", Trancommit);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommitAsync", TrancommitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", Tranabort);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabortAsync", TranabortAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", Path);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", Rnum);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fsiz", Fsiz);

      target->Set(String::New("HDB"), tmpl->GetFunction());
    }

  private:
    TCHDB *db;

    class AsyncData : public AsyncDataCore {
      public:
        HDB *hdb;

        // init() may be overridden by ArgsData variants
        void
        init (const Arguments& args) {
          setCallback(THIS, ARG0);
        }

        // maybe use 'This' as 'this' of callback function. not sure yet.
        void
        setCallback (Handle<Object> This, Handle<Value> cb) {
          AsyncDataCore::setCallback(cb);
          hdb = Unwrap(This);
          hdb->Ref();
        }

        virtual
        ~AsyncData () {
          hdb->Unref();
        }

        int
        ecode () {
          return tchdbecode(hdb->db);
        }
    };

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (!args.IsConstructCall()) return args.Callee()->NewInstance();
      (new HDB)->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Errmsg (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsUndefined() || ARG0->IsNumber())) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tchdberrmsg(
          ARG0->IsUndefined() ? tchdbecode(Backend(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tchdbecode(Backend(THIS));
      return Integer::New(ecode);
    }

    static Handle<Value>
    Setmutex (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbsetmutex(
          Backend(THIS));
      return Boolean::New(success);
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
          Backend(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? -1 : VINT32(ARG3));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setcache (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || ARG0->IsUndefined())) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetcache(
          Backend(THIS),
          ARG0->IsUndefined() ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setxmsiz (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || ARG0->IsUndefined())) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetxmsiz(
          Backend(THIS),
          ARG0->IsUndefined() ? -1 : VINT64(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Setdfunit (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || ARG0->IsUndefined())) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbsetdfunit(
          Backend(THIS),
          ARG0->IsUndefined() ? -1 : VINT32(ARG0));
      return Boolean::New(success);
    }

    class OpenData : public AsyncData, public OpenDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG2);
          setPath(ARG0);
          omode = ARG1->IsUndefined() ? HDBOREADER : VINT32(ARG1);
        }

        bool run () {
          return tchdbopen(hdb->db, **(path), omode);
        }
    };

    static Handle<Value>
    Open (const Arguments& args) {
      HandleScope scope;
      if (!OpenData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbopen(
          Backend(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? HDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Open)

    class CloseData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbclose(hdb->db);
        }
    };

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbclose(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Close)

    class PutData : public AsyncData, public PutDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG2);
          setKeyVal(ARG0, ARG1);
        }

        bool run () {
          return tchdbput(hdb->db, **kbuf, ksiz, **vbuf, vsiz);
        }
    };

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (!PutData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbput(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Put)

    class PutkeepData : public PutData {
      public:
        bool run () {
          return tchdbputkeep(hdb->db, **kbuf, ksiz, **vbuf, vsiz);
        }
    };

    static Handle<Value>
    Putkeep (const Arguments& args) {
      HandleScope scope;
      if (!PutkeepData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputkeep(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Putkeep)

    class PutcatData : public PutData {
      public:
        bool run () {
          return tchdbputcat(hdb->db, **kbuf, ksiz, **vbuf, vsiz);
        }
    };

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (!PutcatData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputcat(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Putcat)

    class PutasyncData : public PutData {
      public:
        bool run () {
          return tchdbputasync(hdb->db, **kbuf, ksiz, **vbuf, vsiz);
        }
    };

    static Handle<Value>
    Putasync (const Arguments& args) {
      HandleScope scope;
      if (!PutasyncData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbputasync(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Putasync)

    class OutData : public AsyncData, public KeyDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG1);
          setKey(ARG0);
        }

        bool run () {
          return tchdbout(hdb->db, **(kbuf), ksiz);
        }
    };

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (!OutData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdbout(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Out)

    class GetData : public AsyncData, public GetDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG1);
          setKey(ARG0);
        }

        bool run () {
          vbuf = static_cast<char *>(tchdbget(hdb->db, **kbuf, ksiz, &vsiz));
          return vbuf != NULL;
        }
    };

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (!GetData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      char *vstr = static_cast<char *>(tchdbget(
          Backend(THIS),
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

    DEFINE_ASYNC2(Get)

    class VsizData : public AsyncData, public VsizDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG1);
          setKey(ARG0);
        }

        bool run () {
          vsiz = tchdbvsiz(hdb->db, **kbuf, ksiz);
          return vsiz != -1;
        }
    };

    static Handle<Value>
    Vsiz (const Arguments& args) {
      HandleScope scope;
      if (!VsizData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      int vsiz = tchdbvsiz(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    DEFINE_ASYNC2(Vsiz)

    class IterinitData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbiterinit(hdb->db);
        }
    };

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbiterinit(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Iterinit)

    class IternextData : public AsyncData, public ValueDataCore {
      public:
        bool run () {
          vbuf = static_cast<char *>(tchdbiternext(hdb->db, &vsiz));
          return vbuf != NULL;
        }
    };

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *vstr = static_cast<char *>(tchdbiternext(
          Backend(THIS),
          &vsiz));
      if (vstr == NULL) {
        return Null();
      } else {
        Local<String> ret = String::New(vstr, vsiz);
        tcfree(vstr);
        return ret;
      }
    }

    DEFINE_ASYNC2(Iternext)

    class FwmkeysData : public AsyncData, public FwmkeysDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG2);
          setKey(ARG0);
          max = NOU(ARG1) ? -1 : VINT32(ARG1);
        }

        bool run () {
          list = tchdbfwmkeys(hdb->db, kbuf, ksiz, max);
          return true;
        }
    };

    static Handle<Value>
    Fwmkeys (const Arguments& args) {
      HandleScope scope;
      if (!FwmkeysData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      TCLIST *keys = tchdbfwmkeys(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      Local<Array> ary = tclisttoary(keys);
      tclistdel(keys);
      return ary;
    }

    DEFINE_ASYNC2(Fwmkeys)

    class AddintData : public AsyncData, public AddintDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG2);
          setKey(ARG0);
          num = VINT32(ARG1);
        }

        bool run () {
          num = tchdbaddint(hdb->db, kbuf, ksiz, num);
          return num != INT_MIN;
        }
    };

    static Handle<Value>
    Addint (const Arguments& args) {
      HandleScope scope;
      if (!AddintData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      int sum = tchdbaddint(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VINT32(ARG1));
      return sum == INT_MIN ? Null() : Integer::New(sum);
    }

    DEFINE_ASYNC2(Addint)

    class AdddoubleData : public AsyncData, public AdddoubleDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG2);
          setKey(ARG0);
          num = VINT32(ARG1);
        }

        bool run () {
          num = tchdbadddouble(hdb->db, kbuf, ksiz, num);
          return !isnan(num);
        }
    };

    static Handle<Value>
    Adddouble (const Arguments& args) {
      HandleScope scope;
      if (!AdddoubleData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      double sum = tchdbadddouble(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    DEFINE_ASYNC2(Adddouble)

    class SyncData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbsync(hdb->db);
        }
    };

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbsync(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Sync)

    class OptimizeData : public AsyncData, public ArgsDataCore {
      private:
        int64_t bnum;
        int8_t apow;
        int8_t fpow;
        uint8_t opts;

      public:
        static bool checkArgs (const Arguments& args) {
          return (ARG0->IsNumber() || NOU(ARG0)) &&
                 (ARG1->IsNumber() || NOU(ARG1)) &&
                 (ARG2->IsNumber() || NOU(ARG2)) &&
                 (ARG3->IsNumber() || NOU(ARG3));
        }

        void init (const Arguments& args) {
          setCallback(THIS, ARG4);
          bnum = NOU(ARG0) ? -1 : VINT64(ARG0);
          apow = NOU(ARG1) ? -1 : VINT32(ARG1);
          fpow = NOU(ARG2) ? -1 : VINT32(ARG2);
          opts = NOU(ARG3) ? UINT8_MAX : VINT32(ARG3);
        }

        bool run () {
          return tchdboptimize(hdb->db, bnum, apow, fpow, opts);
        }
    };

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!OptimizeData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      bool success = tchdboptimize(
          Backend(THIS),
          NOU(ARG0) ? -1 : VINT64(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1),
          NOU(ARG2) ? -1 : VINT32(ARG2),
          NOU(ARG3) ? UINT8_MAX : VINT32(ARG3));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Optimize)

    class VanishData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbvanish(hdb->db);
        }
    };

    DEFINE_ASYNC(Vanish)

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbvanish(
          Backend(THIS));
      return Boolean::New(success);
    }

    class CopyData : public AsyncData, public PathDataCore {
      public:
        void init (const Arguments& args) {
          setCallback(THIS, ARG1);
          setPath(ARG0);
        }

        bool run () {
          return tchdbcopy(hdb->db, **path);
        }
    };

    static Handle<Value>
    Copy (const Arguments& args) {
      HandleScope scope;
      if (!CopyData::checkArgs(args)) {
        return THROW_BAD_ARGS;
      }
      int success = tchdbcopy(
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Copy)

    class TranbeginData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbtranbegin(hdb->db);
        }
    };

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtranbegin(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Tranbegin)

    class TrancommitData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbtrancommit(hdb->db);
        }
    };

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtrancommit(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Trancommit)

    class TranabortData : public AsyncData, public ArgsDataCore {
      public:
        bool run () {
          return tchdbtranabort(hdb->db);
        }
    };

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tchdbtranabort(
          Backend(THIS));
      return Boolean::New(success);
    }

    DEFINE_ASYNC(Tranabort)

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tchdbpath(
          Backend(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tchdbrnum(
          Backend(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tchdbfsiz(
          Backend(THIS));
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

    static BDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<BDB>(obj);
    }

    static TCBDB *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->db;
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
      if (!(NOU(ARG0) || ARG0->IsNumber())) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tchdberrmsg(
          NOU(ARG0) ? tcbdbecode(Backend(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tcbdbecode(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? BDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbclose(Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG0->IsString() ||
          !ARG1->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbput(
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putcat (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG0->IsString() ||
          !ARG1->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbputcat(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VSTRPTR(ARG1),
          VSTRSIZ(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Putdup (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2 ||
          !ARG0->IsString() ||
          !ARG1->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbputdup(
          Backend(THIS),
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
          !ARG0->IsString() ||
          !ARG1->IsArray()) {
        return THROW_BAD_ARGS;
      }
      TCLIST *list = arytotclist(Local<Array>::Cast(ARG1));
      bool success = tcbdbputdup3(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          list);
      tclistdel(list);
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbout(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Outlist (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbout3(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Get (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      int vsiz;
      const char *vstr = static_cast<const char *>(tcbdbget3(
          Backend(THIS),
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
      if (args.Length() < 1 ||
          !ARG0->IsString()) {
        return THROW_BAD_ARGS;
      }
      TCLIST *list = tcbdbget4(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Range (const Arguments& args) {
      HandleScope scope;
      TCLIST *list = tcbdbrange(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbsync(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtranbegin(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtrancommit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbtranabort(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcbdbpath(
          Backend(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcbdbrnum(
          Backend(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcbdbfsiz(
          Backend(THIS));
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

    static CUR *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<CUR>(obj);
    }

    static BDBCUR *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->cur;
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
      TCBDB *db = BDB::Backend(Local<Object>::Cast(ARG0));
      (new CUR(db))->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    First (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurfirst(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Last (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurlast(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Jump (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 1) {
        return THROW_BAD_ARGS;
      }
      bool success = tcbdbcurjump(
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Prev (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurprev(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Next (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurnext(
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          NOU(ARG1) ? BDBCPCURRENT : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Out (const Arguments& args) {
      HandleScope scope;
      bool success = tcbdbcurout(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Key (const Arguments& args) {
      HandleScope scope;
      int ksiz;
      char *key = static_cast<char *>(tcbdbcurkey(
          Backend(THIS),
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
          Backend(THIS),
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

    static FDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<FDB>(obj);
    }

    static TCFDB *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->db;
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
      if (!(NOU(ARG0) || ARG0->IsNumber())) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tcfdberrmsg(
          NOU(ARG0) ? tcfdbecode(Backend(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tcfdbecode(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? FDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbclose(Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcfdbput2(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbiterinit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *vstr = static_cast<char *>(tcfdbiternext2(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          tcfdbkeytoid(VSTRPTR(ARG0), VSTRSIZ(ARG0)),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbsync(
          Backend(THIS));
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
          Backend(THIS),
          NOU(ARG0) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT64(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbvanish(
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtranbegin(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtrancommit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcfdbtranabort(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcfdbpath(
          Backend(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcfdbrnum(
          Backend(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcfdbfsiz(
          Backend(THIS));
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

    static TDB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<TDB>(obj);
    }

    static TCTDB *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->db;
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
      if (!(NOU(ARG0) || ARG0->IsNumber())) {
        return THROW_BAD_ARGS;
      }
      const char *msg = tctdberrmsg(
          NOU(ARG0) ? tctdbecode(Backend(THIS)) : VINT32(ARG0));
      return String::New(msg);
    }

    static Handle<Value>
    Ecode (const Arguments& args) {
      HandleScope scope;
      int ecode = tctdbecode(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          NOU(ARG1) ? TDBOREADER : VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbclose(Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbiterinit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      int vsiz;
      char *vstr = static_cast<char *>(tctdbiternext(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbsync(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtranbegin(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtrancommit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbtranabort(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tctdbpath(
          Backend(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tctdbrnum(
          Backend(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Fsiz (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tctdbfsiz(
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VINT32(ARG1));
      return Boolean::New(success);
    }

    static Handle<Value>
    Genuid (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tctdbgenuid(
          Backend(THIS));
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

    static QRY *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<QRY>(obj);
    }

    static TDBQRY *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->qry;
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
      TCTDB *db = TDB::Backend(Local<Object>::Cast(ARG0));
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          NOU(ARG1) ? -1 : VINT32(ARG0),
          NOU(ARG1) ? -1 : VINT32(ARG1));
      return Undefined();
    }

    static Handle<Value>
    Search (const Arguments& args) {
      HandleScope scope;
      TCLIST *res = tctdbqrysearch(
          Backend(THIS));
      Local<Array> ary = tclisttoary(res);
      tclistdel(res);
      return ary;
    }

    static Handle<Value>
    Searchout (const Arguments& args) {
      HandleScope scope;
      bool success = tctdbqrysearchout(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Hint (const Arguments& args) {
      HandleScope scope;
      const char *hint = tctdbqryhint(
          Backend(THIS));
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
      qrys[qnum++] = Backend(THIS);
      Local<Value> oqry;
      for (int i = 0; i < num; i++) {
        oqry = others->Get(Integer::New(i));
        if (TDB::Tmpl->HasInstance(oqry)) {
          qrys[qnum++] = Backend(Local<Object>::Cast(oqry));
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

    static ADB *
    Unwrap (const Handle<Object> obj) {
      return ObjectWrap::Unwrap<ADB>(obj);
    }

    static TCADB *
    Backend (const Handle<Object> obj) {
      return Unwrap(obj)->db;
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
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Close (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbclose(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Put (const Arguments& args) {
      HandleScope scope;
      if (args.Length() < 2) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadbput(
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0));
      return Integer::New(vsiz);
    }

    static Handle<Value>
    Iterinit (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbiterinit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Iternext (const Arguments& args) {
      HandleScope scope;
      char *vstr = tcadbiternext2(
          Backend(THIS));
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
          Backend(THIS),
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
          Backend(THIS),
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
          Backend(THIS),
          VSTRPTR(ARG0),
          VSTRSIZ(ARG0),
          VDOUBLE(ARG1));
      return isnan(sum) ? Null() : Number::New(sum);
    }

    static Handle<Value>
    Sync (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbsync(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Optimize (const Arguments& args) {
      HandleScope scope;
      if (!(ARG0->IsNumber() || NOU(ARG0))) {
        return THROW_BAD_ARGS;
      }
      bool success = tcadboptimize(
          Backend(THIS),
          NOU(ARG0) ? NULL : VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Vanish (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbvanish(
          Backend(THIS));
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
          Backend(THIS),
          VSTRPTR(ARG0));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranbegin (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtranbegin(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Trancommit (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtrancommit(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Tranabort (const Arguments& args) {
      HandleScope scope;
      bool success = tcadbtranabort(
          Backend(THIS));
      return Boolean::New(success);
    }

    static Handle<Value>
    Path (const Arguments& args) {
      HandleScope scope;
      const char *path = tcadbpath(
          Backend(THIS));
      return path == NULL ? Null() : String::New(path);
    }

    static Handle<Value>
    Rnum (const Arguments& args) {
      HandleScope scope;
      int64_t num = tcadbrnum(
          Backend(THIS));
      return Integer::New(num);
    }

    static Handle<Value>
    Size (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tcadbsize(
          Backend(THIS));
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
          Backend(THIS),
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
  target->Set(String::NewSymbol("VERSION"), String::New(tcversion));
}

