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
#include <assert.h>
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
  return scope.Close(ary);
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
  HandleScope scope;
  const char *kbuf, *vbuf;
  int ksiz, vsiz;
  Local<Object> obj = Object::New();
  tcmapiterinit(map);
  for (;;) {
    kbuf = static_cast<const char*>(tcmapiternext(map, &ksiz));
    if (kbuf == NULL) break;
    vbuf = static_cast<const char*>(tcmapiterval(kbuf, &vsiz));
    obj->Set(String::New(kbuf, ksiz), String::New(vbuf, vsiz));
  }
  return scope.Close(obj);
}

/* sync method blueprint */
#define DEFINE_SYNC(name)                                                     \
  static Handle<Value>                                                        \
  name##Sync (const Arguments& args) {                                        \
    HandleScope scope;                                                        \
    if (!name##Data::checkArgs(args)) {                                       \
      return THROW_BAD_ARGS;                                                  \
    }                                                                         \
    return scope.Close(Boolean::New(name##Data(args).run()));                 \
  }                                                                           \

/* when there is an extra value to return */
#define DEFINE_SYNC2(name)                                                    \
  static Handle<Value>                                                        \
  name##Sync (const Arguments& args) {                                        \
    HandleScope scope;                                                        \
    if (!name##Data::checkArgs(args)) {                                       \
      return THROW_BAD_ARGS;                                                  \
    }                                                                         \
    name##Data data(args);                                                    \
    data.run();                                                               \
    return scope.Close(data.returnValue());                                   \
  }                                                                           \

/* async method blueprint */
#define DEFINE_ASYNC_FUNC(name)                                               \
  static Handle<Value>                                                        \
  name##Async (const Arguments& args) {                                       \
    HandleScope scope;                                                        \
    if (!name##Data::checkArgs(args)) {                                       \
      return THROW_BAD_ARGS;                                                  \
    }                                                                         \
    name##AsyncData *data = new name##AsyncData(args);                        \
    eio_custom(Exec##name, EIO_PRI_DEFAULT, After##name, data);               \
    ev_ref(EV_DEFAULT_UC);                                                    \
    return Undefined();                                                       \
  }                                                                           \

#define DEFINE_ASYNC_EXEC(name)                                               \
  static int                                                                  \
  Exec##name (eio_req *req) {                                                 \
    name##AsyncData *data = static_cast<name##AsyncData *>(req->data);        \
    req->result = data->run() ? TCESUCCESS : data->ecode();                   \
    return 0;                                                                 \
  }                                                                           \

#define DEFINE_ASYNC_AFTER(name)                                              \
  static int                                                                  \
  After##name (eio_req *req) {                                                \
    HandleScope scope;                                                        \
    name##AsyncData *data = static_cast<name##AsyncData *>(req->data);        \
    if (data->hasCallback) {                                                  \
      data->callCallback(Integer::New(req->result));                          \
    }                                                                         \
    ev_unref(EV_DEFAULT_UC);                                                  \
    delete data;                                                              \
    return 0;                                                                 \
  }

/* when there is an extra value to return */
#define DEFINE_ASYNC_AFTER2(name)                                             \
  static int                                                                  \
  After##name (eio_req *req) {                                                \
    HandleScope scope;                                                        \
    name##AsyncData *data = static_cast<name##AsyncData *>(req->data);        \
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

// Tokyo Cabinet error codes
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

// Database wrapper (interfaces for database objects, all included)
class TCWrap : public ObjectWrap {
  public:
    // these methods must be overridden in individual DB classes
    virtual int Ecode () { assert(false); }
    virtual const char * Errmsg (int ecode) { assert(false); }
    virtual bool Setmutex () { assert(false); }
    virtual bool Tune (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) { assert(false); } // for HDB
    virtual bool Tune (int32_t lmemb, int32_t nmemb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) { assert(false); } // for BDB
    virtual bool Tune (int32_t width, int64_t limsiz) { assert(false); } // for FDB
    virtual bool Setcache (int32_t rcnum) { assert(false); } // for HDB
    virtual bool Setcache (int32_t lcnum, int32_t ncnum) { assert(false); } // for BDB
    virtual bool Setcache (int32_t rcnum, int32_t lcnum, int32_t ncnum) { assert(false); } // for TDB
    virtual bool Setxmsiz (int64_t xmsiz) { assert(false); }
    virtual bool Setdfunit (int32_t dfunit) { assert(false); }
    virtual bool Open (char *path, int omode) { assert(false); }
    virtual bool Close () { assert(false); }
    virtual bool Put(char *kbuf, int ksiz, char *vbuf, int vsiz) { assert(false); }
    virtual bool Put(char *kbuf, int ksiz, TCMAP *map) { assert(false); } // for TDB
    virtual bool Putkeep(char *kbuf, int ksiz, char *vbuf, int vsiz) { assert(false); }
    virtual bool Putkeep(char *kbuf, int ksiz, TCMAP *map) { assert(false); } // for TDB
    virtual bool Putcat(char *kbuf, int ksiz, char *vbuf, int vsiz) { assert(false); }
    virtual bool Putcat(char *kbuf, int ksiz, TCMAP *map) { assert(false); } // for TDB
    virtual bool Putasync(char *kbuf, int ksiz, char *vbuf, int vsiz) { assert(false); }
    virtual bool Putdup(char *kbuf, int ksiz, char *vbuf, int vsiz) { assert(false); } // for BDB
    virtual bool Putlist(char *kbuf, int ksiz, TCLIST *list) { assert(false); } // for BDB
    virtual bool Out(char *kbuf, int ksiz) { assert(false); }
    virtual bool Outlist(char *kbuf, int ksiz) { assert(false); } // for BDB
    virtual char * Get(char *kbuf, int ksiz, int *vsiz_p) { assert(false); }
    virtual TCMAP * Get(char *kbuf, int ksiz) { assert(false); } // for TDB
    virtual TCLIST * Getlist(char *kbuf, int ksiz) { assert(false); } // for BDB
    virtual int Vnum (char *kbuf, int ksiz) { assert(false); }
    virtual int Vsiz(char *kbuf, int ksiz) { assert(false); }
    virtual TCLIST * Range(char *bkbuf, int bksiz, bool binc, char *ekbuf, int eksiz, bool einc, int max) { assert(false); } // for BDB
    virtual TCLIST * Range(char *ibuf, int isiz, int max) { assert(false); } // for FDB
    virtual bool Iterinit () { assert(false); }
    virtual char * Iternext (int *vsiz_p) { assert(false); }
    virtual TCLIST * Fwmkeys(char *kbuf, int ksiz, int max) { assert(false); }
    virtual int Addint(char *kbuf, int ksiz, int num) { assert(false); }
    virtual double Adddouble(char *kbuf, int ksiz, double num) { assert(false); }
    virtual bool Sync () { assert(false); }
    virtual bool Optimize (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) { assert(false); } // for HDB
    virtual bool Optimize (int32_t lmemb, int32_t nmemb, int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) { assert(false); } // for HDB
    virtual bool Optimize (int32_t width, int64_t limsiz) { assert(false); } // for FDB
    virtual bool Vanish () { assert(false); }
    virtual bool Copy (char *path) { assert(false); }
    virtual bool Tranbegin () { assert(false); }
    virtual bool Trancommit () { assert(false); }
    virtual bool Tranabort () { assert(false); }
    virtual const char* Path () { assert(false); }
    virtual uint64_t Rnum () { assert(false); }
    virtual uint64_t Fsiz () { assert(false); }
    virtual bool Setindex (const char* name, int type) { assert(false); } // for TDB
    // for BDB Cursor
    virtual bool First () { assert(false); } // for CUR
    virtual bool Last () { assert(false); } // for CUR
    virtual bool Jump (char *kbuf, int ksiz) { assert(false); } // for CUR
    virtual bool Prev () { assert(false); } // for CUR
    virtual bool Next () { assert(false); } // for CUR
    virtual bool Put (char *kbuf, int ksiz, int cpmode) { assert(false); } // for CUR
    virtual bool Out () { assert(false); } // for CUR
    virtual char * Key (int *vsiz_p) { assert(false); } // for CUR
    virtual char * Val (int *vsiz_p) { assert(false); } // for CUR
    // for TDB Query
    virtual TCLIST * Search () { assert(false); } // for QRY
    virtual bool Searchout () { assert(false); } // for QRY
    virtual TCLIST * Metasearch (TDBQRY **qrys, int num, int type) { assert(false); } // for QRY

  protected:
    class ArgsData {
      protected:
        TCWrap *tcw;

        /* This is defined only because AsyncData(Handle<Value>) constructor 
         * gives compile error without this, but in fact AsyncData does not
         * use it. See OpenAsyncData or GetData, for example. */
        ArgsData () {
          assert(false);
        }

      public:
        static bool 
        checkArgs (const Arguments& args) {
          return true;
        }

        ArgsData (const Arguments& args) {
          tcw = Unwrap<TCWrap>(args.This());
        }

        int
        ecode () {
          return tcw->Ecode();
        }
    };

    class AsyncData : public virtual ArgsData {
      public:
        Persistent<Function> cb;
        bool hasCallback;

        AsyncData (Handle<Value> cb_) {
          HandleScope scope;
          assert(tcw); // make sure ArgsData is already initialized with This value
          tcw->Ref();
          if (cb_->IsFunction()) {
            hasCallback = true;
            cb = Persistent<Function>::New(Handle<Function>::Cast(cb_));
          } else {
            hasCallback = false;
          }
        }

        virtual 
        ~AsyncData () {
          tcw->Unref();
          cb.Dispose();
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
          Handle<Value> args[1] = {arg0};
          callback(1, args);
        }

        inline void
        callCallback (Handle<Value> arg0, Handle<Value>arg1) {
          HandleScope scope;
          Handle<Value> args[2] = {arg0, arg1};
          callback(2, args);
        }
    };

    class EcodeData : public ArgsData {
      private:
        int ecode;

      public:
        EcodeData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          ecode = tcw->Ecode();
          return true;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(Integer::New(ecode));
        }
    };

    class ErrmsgData : public ArgsData {
      public:
        int ecode;
        const char *msg;

        ErrmsgData (const Arguments& args) : ArgsData(args) {
          ecode = ARG0->IsNumber() ? ARG0->Int32Value() : tcw->Ecode();
        }

        bool 
        run () {
          msg = tcw->Errmsg(ecode);
          return true;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(String::New(msg));
        }

        static bool 
        checkArgs (const Arguments& args) {
          return ARG0->IsNumber() || ARG0->IsUndefined();
        }
    };

    class SetmutexData : public ArgsData {
      public:
        SetmutexData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Setmutex();
        }
    };

    class SetxmsizData : public ArgsData {
      private:
        int64_t xmsiz;

      public:
        SetxmsizData (const Arguments& args) : ArgsData(args) {
          xmsiz = ARG0->IsUndefined() ? -1 : ARG0->IntegerValue();
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG0->IsNumber() || ARG0->IsUndefined();
        }

        bool
        run () {
          return tcw->Setxmsiz(xmsiz);
        }
    };

    class SetdfunitData : public ArgsData {
      private:
        int32_t dfunit;

      public:
        SetdfunitData (const Arguments& args) : ArgsData(args) {
          dfunit = ARG0->IsUndefined() ? -1 : ARG0->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG0->IsNumber() || ARG0->IsUndefined();
        }

        bool
        run () {
          return tcw->Setdfunit(dfunit);
        }
    };

    class FilenameData : public virtual ArgsData {
      protected:
        String::Utf8Value path;

      public:
        FilenameData (const Arguments& args) : path(args[0]), ArgsData(args) {}

        static bool
        checkArgs (const Arguments& args) {
          return ARG0->IsString();
        }
    };

    class CloseData : public virtual ArgsData {
      public:
        CloseData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Close();
        }
    };

    class CloseAsyncData : public CloseData, public AsyncData {
      public:
        CloseAsyncData (const Arguments& args)
          : CloseData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class KeyData : public virtual ArgsData {
      protected:
        String::Utf8Value kbuf;
        int ksiz;

      public:
        KeyData (const Arguments& args) : kbuf(args[0]), ArgsData(args) {
          ksiz = kbuf.length();
        }
    };

    class PutData : public KeyData {
      protected:
        String::Utf8Value vbuf;
        int vsiz;

      public:
        PutData (const Arguments& args) : vbuf(args[1]), KeyData(args) {
          vsiz = vbuf.length();
        }

        bool
        run () {
          return tcw->Put(*kbuf, ksiz, *vbuf, vsiz);
        }
    };

    class PutAsyncData : public PutData, public AsyncData {
      public:
        PutAsyncData (const Arguments& args)
          : PutData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class PutkeepData : public PutData {
      public:
        PutkeepData (const Arguments& args) : PutData(args) {}

        bool
        run () {
          return tcw->Putkeep(*kbuf, ksiz, *vbuf, vsiz);
        }
    };

    class PutkeepAsyncData : public PutkeepData, public AsyncData {
      public:
        PutkeepAsyncData (const Arguments& args)
          : PutkeepData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class PutcatData : public PutData {
      public:
        PutcatData (const Arguments& args) : PutData(args) {}

        bool
        run () {
          return tcw->Putcat(*kbuf, ksiz, *vbuf, vsiz);
        }
    };

    class PutcatAsyncData : public PutcatData, public AsyncData {
      public:
        PutcatAsyncData (const Arguments& args)
          : PutcatData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class PutasyncData : public PutData {
      public:
        PutasyncData (const Arguments& args) : PutData(args) {}

        bool
        run () {
          return tcw->Putasync(*kbuf, ksiz, *vbuf, vsiz);
        }
    };

    class PutasyncAsyncData : public PutasyncData, public AsyncData {
      public:
        PutasyncAsyncData (const Arguments& args)
          : PutasyncData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class PutdupData : public PutData {
      public:
        PutdupData (const Arguments& args) : PutData(args) {}

        bool
        run () {
          return tcw->Putdup(*kbuf, ksiz, *vbuf, vsiz);
        }
    };

    class PutdupAsyncData : public PutdupData, public AsyncData {
      public:
        PutdupAsyncData (const Arguments& args)
          : PutdupData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class PutlistData : public KeyData {
      private:
        TCLIST *list;

      public:
        PutlistData (const Arguments& args) : KeyData(args) {
          HandleScope scope;
          list = arytotclist(Handle<Array>::Cast(ARG0));
        }

        ~PutlistData () {
          tclistdel(list);
        }

        bool
        run () {
          return tcw->Putlist(*kbuf, ksiz, list);
        }
    };

    class PutlistAsyncData : public PutlistData, public AsyncData {
      public:
        PutlistAsyncData (const Arguments& args)
          : PutlistData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class ValueData : public virtual ArgsData {
      protected:
        char *vbuf;
        int vsiz;

      public:
        ~ValueData () {
          tcfree(vbuf);
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return vbuf == NULL ? Null() : scope.Close(String::New(vbuf, vsiz));
        }
    };

    class OutData : public KeyData {
      public:
        OutData (const Arguments& args) : KeyData(args) {}

        bool
        run () {
          return tcw->Out(*kbuf, ksiz);
        }
    };

    class OutAsyncData : public OutData, public AsyncData {
      public:
        OutAsyncData (const Arguments& args)
          : OutData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class OutlistData : public KeyData {
      public:
        OutlistData (const Arguments& args) : KeyData(args) {}

        bool
        run () {
          return tcw->Outlist(*kbuf, ksiz);
        }
    };

    class OutlistAsyncData : public OutlistData, public AsyncData {
      public:
        OutlistAsyncData (const Arguments& args)
          : OutlistData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class GetData : public KeyData, public ValueData {
      public:
        GetData (const Arguments& args) : KeyData(args), ArgsData(args) {}

        bool
        run () {
          vbuf = tcw->Get(*kbuf, ksiz, &vsiz);
          return vbuf != NULL;
        }
    };

    class GetAsyncData : public GetData, public AsyncData {
      public:
        GetAsyncData (const Arguments& args)
          : GetData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class GetlistData : public KeyData {
      protected:
        TCLIST *list;

      public:
        GetlistData (const Arguments& args) : KeyData(args) {}

        ~GetlistData () {
          tclistdel(list);
        }

        bool
        run () {
          list = tcw->Getlist(*kbuf, ksiz);
          return true;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(tclisttoary(list));
        }
    };

    class GetlistAsyncData : public GetlistData, public AsyncData {
      public:
        GetlistAsyncData (const Arguments& args)
          : GetlistData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class FwmkeysData : public GetlistData {
      protected:
        int max;

      public:
        FwmkeysData (const Arguments& args) : GetlistData(args) {
          max = args[1]->Int32Value();
        }

        bool
        run () {
          list = tcw->Fwmkeys(*kbuf, ksiz, max);
          return true;
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG1->IsNumber();
        }
    };

    class FwmkeysAsyncData : public FwmkeysData, public AsyncData {
      public:
        FwmkeysAsyncData (const Arguments& args)
          : FwmkeysData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class VnumData : public KeyData {
      protected:
        int vnum;

      public:
        VnumData (const Arguments& args) : KeyData(args) {}

        bool
        run () {
          vnum = tcw->Vnum(*kbuf, ksiz);
          return vnum != 0;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(Number::New(vnum));
        }
    };

    class VnumAsyncData : public VnumData, public AsyncData {
      public:
        VnumAsyncData (const Arguments& args)
          : VnumData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class VsizData : public KeyData {
      protected:
        int vsiz;

      public:
        VsizData (const Arguments& args) : KeyData(args) {}

        bool
        run () {
          vsiz = tcw->Vsiz(*kbuf, ksiz);
          return vsiz != -1;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(Number::New(vsiz));
        }
    };

    class VsizAsyncData : public VsizData, public AsyncData {
      public:
        VsizAsyncData (const Arguments& args)
          : VsizData(args), AsyncData(args[1]), ArgsData(args) {}
    };

    class AddintData : public KeyData {
      protected:
        int num;

      public:
        AddintData (const Arguments& args) : KeyData(args) {
          num = args[1]->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG1->IsNumber();
        }

        bool
        run () {
          num = tcw->Addint(*kbuf, ksiz, num);
          return num != INT_MIN;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return num == INT_MIN ? Null() : scope.Close(Integer::New(num));
        }
    };

    class AddintAsyncData : public AddintData, public AsyncData {
      public:
        AddintAsyncData (const Arguments& args)
          : AddintData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class IterinitData : public virtual ArgsData {
      public:
        IterinitData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Iterinit();
        }
    };

    class IterinitAsyncData : public IterinitData, public AsyncData {
      public:
        IterinitAsyncData (const Arguments& args)
          : IterinitData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class IternextData : public ValueData {
      public:
        IternextData (const Arguments& args) {}

        bool run () {
          vbuf = tcw->Iternext(&vsiz);
          return vbuf != NULL;
        }
    };

    class IternextAsyncData : public IternextData, public AsyncData {
      public:
        IternextAsyncData (const Arguments& args)
          : IternextData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class AdddoubleData : public KeyData {
      protected:
        double num;

      public:
        AdddoubleData (const Arguments& args) : KeyData(args) {
          num = args[1]->NumberValue();
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG1->IsNumber();
        }

        bool
        run () {
          num = tcw->Adddouble(*kbuf, ksiz, num);
          return isnan(num);
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return isnan(num) ? Null() : scope.Close(Number::New(num));
        }
    };

    class AdddoubleAsyncData : public AdddoubleData, public AsyncData {
      public:
        AdddoubleAsyncData (const Arguments& args)
          : AdddoubleData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class SyncData : public virtual ArgsData {
      public:
        SyncData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Sync();
        }
    };

    class SyncAsyncData : public SyncData, public AsyncData {
      public:
        SyncAsyncData (const Arguments& args)
          : SyncData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class VanishData : public virtual ArgsData {
      public:
        VanishData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Vanish();
        }
    };

    class VanishAsyncData : public VanishData, public AsyncData {
      public:
        VanishAsyncData (const Arguments& args)
          : VanishData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class CopyData : public FilenameData {
      public:
        CopyData (const Arguments& args) : FilenameData(args) {}

        bool
        run () {
          return tcw->Copy(*path);
        }
    };

    class CopyAsyncData : public CopyData, public AsyncData {
      public:
        CopyAsyncData (const Arguments& args)
          : CopyData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    class TranbeginData : public virtual ArgsData {
      public:
        TranbeginData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Tranbegin();
        }
    };

    class TranbeginAsyncData : public TranbeginData, public AsyncData {
      public:
        TranbeginAsyncData (const Arguments& args)
          : TranbeginData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class TrancommitData : public virtual ArgsData {
      public:
        TrancommitData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Trancommit();
        }
    };

    class TrancommitAsyncData : public TrancommitData, public AsyncData {
      public:
        TrancommitAsyncData (const Arguments& args)
          : TrancommitData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class TranabortData : public virtual ArgsData {
      public:
        TranabortData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Tranabort();
        }
    };

    class TranabortAsyncData : public TranabortData, public AsyncData {
      public:
        TranabortAsyncData (const Arguments& args)
          : TranabortData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    class PathData : public ArgsData {
      private:
        const char *path;

      public:
        PathData (const Arguments& args) : ArgsData(args) {}

        bool
        run () {
          path = tcw->Path();
          return path != NULL;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return path == NULL ? Null() : scope.Close(String::New(path));
        }
    };

    class RnumData : public ArgsData {
      private:
        uint64_t rnum;

      public:
        RnumData (const Arguments& args) : ArgsData(args) {}

        bool
        run () {
          rnum = tcw->Rnum();
          // rnum == 0 when not connected to any database file
          return rnum != 0;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          // JavaScript integer cannot express uint64
          // so this can't handle too large number
          return scope.Close(Integer::New(rnum));
        }
    };

    class FsizData : public ArgsData {
      private:
        uint64_t fsiz;

      public:
        FsizData (const Arguments& args) : ArgsData(args) {}

        bool
        run () {
          fsiz = tcw->Fsiz();
          // fsiz == 0 when not connected to any database file
          return fsiz != 0;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          // JavaScript integer cannot express uint64
          // so this can't handle too large number
          return scope.Close(Integer::New(fsiz));
        }
    };
};

class HDB : public TCWrap {
  public:
    HDB () {
      hdb = tchdbnew();
    }

    ~HDB () {
      tchdbdel(hdb);
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

      NODE_SET_PROTOTYPE_METHOD(tmpl, "errmsg", ErrmsgSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "ecode", EcodeSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setmutex", SetmutexSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tune", TuneSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setcache", SetcacheSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setxmsiz", SetxmsizSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setdfunit", SetdfunitSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", OpenSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "openAsync", OpenAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", CloseSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "closeAsync", CloseAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", PutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", PutkeepSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeepAsync", PutkeepAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", PutcatSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcatAsync", PutcatAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putasync", PutasyncSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putasyncAsync", PutasyncAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", OutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", GetSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "getAsync", GetAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", VsizSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsizAsync", VsizAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", IterinitSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinitAsync", IterinitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", IternextSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternextAsync", IternextAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeys", FwmkeysSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fwmkeysAsync", FwmkeysAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", AddintSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addintAsync", AddintAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", AdddoubleSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddoubleAsync", AdddoubleAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", SyncSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "syncAsync", SyncAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", OptimizeSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimizeAsync", OptimizeAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", VanishSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanishAsync", VanishAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", CopySync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copyAsync", CopyAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", TranbeginSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbeginAsync", TranbeginAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", TrancommitSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommitAsync", TrancommitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", TranabortSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabortAsync", TranabortAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", PathSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", RnumSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fsiz", FsizSync);

      target->Set(String::New("HDB"), tmpl->GetFunction());
    }

  private:
    TCHDB *hdb;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (!args.IsConstructCall()) return args.Callee()->NewInstance();
      (new HDB)->Wrap(THIS);
      return THIS;
    }

    int Ecode () {
      return tchdbecode(hdb);
    }

    DEFINE_SYNC2(Ecode)

    const char * Errmsg (int ecode) {
      return tchdberrmsg(ecode);
    }

    DEFINE_SYNC2(Errmsg)

    bool Setmutex () {
      return tchdbsetmutex(hdb);
    }

    DEFINE_SYNC(Setmutex)

    bool Setcache (int32_t rcnum) {
      return tchdbsetcache(hdb, rcnum);
    }

    class SetcacheData : public ArgsData {
      private:
        int32_t rcnum;

      public:
        SetcacheData (const Arguments& args) : ArgsData(args) {
          rcnum = ARG0->IsUndefined() ? -1 : ARG0->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return ARG0->IsNumber() || ARG0->IsUndefined();
        }

        bool
        run () {
          return tcw->Setcache(rcnum);
        }
    };

    DEFINE_SYNC(Setcache)

    bool Setxmsiz (int64_t xmsiz) {
      return tchdbsetxmsiz(hdb, xmsiz);
    }

    DEFINE_SYNC(Setxmsiz)

    bool Setdfunit (int32_t dfunit) {
      return tchdbsetdfunit(hdb, dfunit);
    }

    DEFINE_SYNC(Setdfunit)

    bool Tune (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) {
      return tchdbtune(hdb, bnum, apow, fpow, opts);
    }

    class TuneData : public virtual ArgsData {
      protected:
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

        TuneData (const Arguments& args) : ArgsData(args) {
          bnum = NOU(ARG0) ? -1 : ARG0->IntegerValue();
          apow = NOU(ARG1) ? -1 : ARG1->Int32Value();
          fpow = NOU(ARG2) ? -1 : ARG2->Int32Value();
          opts = NOU(ARG3) ? UINT8_MAX : ARG3->Int32Value();
        }

        bool run () {
          return tcw->Tune(bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Tune)

    bool Open (char *path, int omode) {
      return tchdbopen(hdb, path, omode);
    }

    class OpenData : public FilenameData {
      protected:
        int omode;

      public:
        OpenData (const Arguments& args) : FilenameData(args) {
          omode = NOU(ARG1) ? HDBOREADER : ARG1->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return FilenameData::checkArgs(args) &&
            (NOU(ARG1) || ARG1->IsNumber());
        }

        bool
        run () {
          return tcw->Open(*path, omode);
        }
    };

    DEFINE_SYNC(Open)

    class OpenAsyncData : public OpenData, public AsyncData {
      public:
        OpenAsyncData (const Arguments& args)
          : OpenData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    DEFINE_ASYNC(Open)

    bool Close () {
      return tchdbclose(hdb);
    }

    DEFINE_SYNC(Close)
    DEFINE_ASYNC(Close)

    bool Put(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tchdbput(hdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Put)
    DEFINE_ASYNC(Put)

    bool Putkeep(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tchdbputkeep(hdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putkeep)
    DEFINE_ASYNC(Putkeep)

    bool Putcat(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tchdbputcat(hdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putcat)
    DEFINE_ASYNC(Putcat)

    bool Putasync(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tchdbputasync(hdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putasync)
    DEFINE_ASYNC(Putasync)

    bool Out(char *kbuf, int ksiz) {
      return tchdbout(hdb, kbuf, ksiz);
    }

    DEFINE_SYNC(Out)
    DEFINE_ASYNC(Out)

    char * Get(char *kbuf, int ksiz, int *vsiz_p) {
      return static_cast<char *>(tchdbget(hdb, kbuf, ksiz, vsiz_p));
    }

    DEFINE_SYNC2(Get)
    DEFINE_ASYNC2(Get)

    int Vsiz(char *kbuf, int ksiz) {
      return tchdbvsiz(hdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Vsiz)
    DEFINE_ASYNC2(Vsiz)

    bool Iterinit () {
      return tchdbiterinit(hdb);
    }

    DEFINE_SYNC(Iterinit)
    DEFINE_ASYNC(Iterinit)

    char * Iternext (int *vsiz_p) {
      return static_cast<char *>(tchdbiternext(hdb, vsiz_p));
    }

    DEFINE_SYNC2(Iternext)
    DEFINE_ASYNC2(Iternext)

    TCLIST * Fwmkeys(char *kbuf, int ksiz, int max) {
      return tchdbfwmkeys(hdb, kbuf, ksiz, max);
    }

    DEFINE_SYNC2(Fwmkeys)
    DEFINE_ASYNC2(Fwmkeys)

    int Addint(char *kbuf, int ksiz, int num) {
      return tchdbaddint(hdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Addint)
    DEFINE_ASYNC2(Addint)

    double Adddouble(char *kbuf, int ksiz, double num) {
      return tchdbadddouble(hdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Adddouble)
    DEFINE_ASYNC2(Adddouble)

    bool Sync () {
      return tchdbsync(hdb);
    }

    DEFINE_SYNC(Sync)
    DEFINE_ASYNC(Sync)

    bool Optimize (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) {
      return tchdboptimize(hdb, bnum, apow, fpow, opts);
    }

    class OptimizeData : public TuneData {
      public:
        OptimizeData (const Arguments& args) : TuneData(args) {}

        bool run () {
          return tcw->Optimize(bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Optimize)

    class OptimizeAsyncData : public OptimizeData, public AsyncData {
      public:
        OptimizeAsyncData (const Arguments& args)
          : OptimizeData(args), AsyncData(ARG4) {}
    };

    DEFINE_ASYNC(Optimize)

    bool Vanish () {
      return tchdbvanish(hdb);
    }

    DEFINE_SYNC(Vanish)
    DEFINE_ASYNC(Vanish)

    bool Copy (char *path) {
      return tchdbcopy(hdb, path);
    }

    DEFINE_SYNC(Copy)
    DEFINE_ASYNC(Copy)

    bool Tranbegin () {
      return tchdbtranbegin(hdb);
    }

    DEFINE_SYNC(Tranbegin)
    DEFINE_ASYNC(Tranbegin)

    bool Trancommit () {
      return tchdbtrancommit(hdb);
    }

    DEFINE_SYNC(Trancommit)
    DEFINE_ASYNC(Trancommit)

    bool Tranabort () {
      return tchdbtranabort(hdb);
    }

    DEFINE_SYNC(Tranabort)
    DEFINE_ASYNC(Tranabort)

    const char * Path () {
      return tchdbpath(hdb);
    }

    DEFINE_SYNC2(Path)

    uint64_t Rnum () {
      return tchdbrnum(hdb);
    }

    DEFINE_SYNC2(Rnum)

    uint64_t Fsiz () {
      return tchdbfsiz(hdb);
    }

    DEFINE_SYNC2(Fsiz)
};

class BDB : public TCWrap {
  public:
    TCBDB *bdb;

    const static Persistent<FunctionTemplate> Tmpl;

    BDB () {
      bdb = tcbdbnew();
    }

    ~BDB () {
      tcbdbdel(bdb);
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

      NODE_SET_PROTOTYPE_METHOD(Tmpl, "errmsg", ErrmsgSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "ecode", EcodeSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setmutex", SetmutexSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tune", TuneSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setcache", SetcacheSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setxmsiz", SetxmsizSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setdfunit", SetdfunitSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "open", OpenSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "openAsync", OpenAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "close", CloseSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "closeAsync", CloseAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "put", PutSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeep", PutkeepSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeepAsync", PutkeepAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcat", PutcatSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcatAsync", PutcatAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putdup", PutdupSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putdupAsync", PutdupAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putlist", PutlistSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putlistAsync", PutlistAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "out", OutSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "outlist", OutlistSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "outlistAsync", OutlistAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "get", GetSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "getAsync", GetAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "getlist", GetlistSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "getlistAsync", GetlistAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vnum", VnumSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vnumAsync", VnumAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsiz", VsizSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsizAsync", VsizAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "range", RangeSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "rangeAsync", RangeAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeys", FwmkeysSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeysAsync", FwmkeysAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addint", AddintSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addintAsync", AddintAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddouble", AdddoubleSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddoubleAsync", AdddoubleAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "sync", SyncSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "syncAsync", SyncAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimize", OptimizeSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimizeAsync", OptimizeAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanish", VanishSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanishAsync", VanishAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copy", CopySync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copyAsync", CopyAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbegin", TranbeginSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbeginAsync", TranbeginAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommit", TrancommitSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommitAsync", TrancommitAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabort", TranabortSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabortAsync", TranabortAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "path", PathSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "rnum", RnumSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fsiz", FsizSync);

      target->Set(String::New("BDB"), Tmpl->GetFunction());
    }

  private:

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (!args.IsConstructCall()) return args.Callee()->NewInstance();
      (new BDB)->Wrap(THIS);
      return THIS;
    }

    int Ecode () {
      return tcbdbecode(bdb);
    }

    DEFINE_SYNC2(Ecode)

    const char * Errmsg (int ecode) {
      return tcbdberrmsg(ecode);
    }

    DEFINE_SYNC2(Errmsg)

    bool Setmutex () {
      return tcbdbsetmutex(bdb);
    }

    DEFINE_SYNC(Setmutex)

    virtual bool Tune (int32_t lmemb, int32_t nmemb, int64_t bnum, int8_t apow, 
                                                    int8_t fpow, uint8_t opts) {
      return tcbdbtune(bdb, lmemb, nmemb, bnum, apow, fpow, opts);
    }

    class TuneData : public virtual ArgsData {
      protected:
        int32_t lmemb;
        int32_t nmemb;
        int64_t bnum;
        int8_t apow;
        int8_t fpow;
        uint8_t opts;

      public:
        static bool checkArgs (const Arguments& args) {
          return (ARG0->IsNumber() || NOU(ARG0)) &&
                 (ARG1->IsNumber() || NOU(ARG1)) &&
                 (ARG2->IsNumber() || NOU(ARG2)) &&
                 (ARG3->IsNumber() || NOU(ARG3)) &&
                 (ARG4->IsNumber() || NOU(ARG4)) &&
                 (ARG5->IsNumber() || NOU(ARG5));
        }

        TuneData (const Arguments& args) : ArgsData(args) {
          lmemb = NOU(ARG0) ? -1 : ARG0->Int32Value();
          nmemb = NOU(ARG1) ? -1 : ARG1->Int32Value();
          bnum = NOU(ARG2) ? -1 : ARG2->IntegerValue();
          apow = NOU(ARG3) ? -1 : ARG3->Int32Value();
          fpow = NOU(ARG4) ? -1 : ARG4->Int32Value();
          opts = NOU(ARG5) ? UINT8_MAX : ARG5->Int32Value();
        }

        bool run () {
          return tcw->Tune(lmemb, nmemb, bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Tune)

    bool Setcache (int32_t lcnum, int32_t ncnum) {
      return tcbdbsetcache(bdb, lcnum, ncnum);
    }

    class SetcacheData : public ArgsData {
      private:
        int32_t lcnum;
        int32_t ncnum;

      public:
        SetcacheData (const Arguments& args) : ArgsData(args) {
          lcnum = ARG0->IsUndefined() ? -1 : ARG0->Int32Value();
          ncnum = ARG1->IsUndefined() ? -1 : ARG1->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return (ARG0->IsNumber() || ARG0->IsUndefined()) &&
                 (ARG1->IsNumber() || ARG1->IsUndefined());
        }

        bool
        run () {
          return tcw->Setcache(lcnum, ncnum);
        }
    };

    DEFINE_SYNC(Setcache)

    bool Setxmsiz (int64_t xmsiz) {
      return tcbdbsetxmsiz(bdb, xmsiz);
    }

    DEFINE_SYNC(Setxmsiz)

    bool Setdfunit (int32_t dfunit) {
      return tcbdbsetdfunit(bdb, dfunit);
    }

    DEFINE_SYNC(Setdfunit)

    bool Open (char *path, int omode) {
      return tcbdbopen(bdb, path, omode);
    }

    class OpenData : public FilenameData {
      protected:
        int omode;

      public:
        OpenData (const Arguments& args) : FilenameData(args) {
          omode = NOU(ARG1) ? BDBOREADER : ARG1->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return FilenameData::checkArgs(args) &&
            (NOU(ARG1) || ARG1->IsNumber());
        }

        bool
        run () {
          return tcw->Open(*path, omode);
        }
    };

    DEFINE_SYNC(Open)

    class OpenAsyncData : public OpenData, public AsyncData {
      public:
        OpenAsyncData (const Arguments& args)
          : OpenData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    DEFINE_ASYNC(Open)

    bool Close () {
      return tcbdbclose(bdb);
    }

    DEFINE_SYNC(Close)
    DEFINE_ASYNC(Close)

    bool Put(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcbdbput(bdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Put)
    DEFINE_ASYNC(Put)

    bool Putkeep(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcbdbputkeep(bdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putkeep)
    DEFINE_ASYNC(Putkeep)

    bool Putcat(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcbdbputcat(bdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putcat)
    DEFINE_ASYNC(Putcat)

    bool Putdup(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcbdbputdup(bdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putdup)
    DEFINE_ASYNC(Putdup)

    bool Putlist(char *kbuf, int ksiz, const TCLIST *vals) {
      return tcbdbputdup3(bdb, kbuf, ksiz, vals);
    }

    DEFINE_SYNC(Putlist)
    DEFINE_ASYNC(Putlist)

    bool Out(char *kbuf, int ksiz) {
      return tcbdbout(bdb, kbuf, ksiz);
    }

    DEFINE_SYNC(Out)
    DEFINE_ASYNC(Out)

    bool Outlist(char *kbuf, int ksiz) {
      return tcbdbout3(bdb, kbuf, ksiz);
    }

    DEFINE_SYNC(Outlist)
    DEFINE_ASYNC(Outlist)

    char * Get(char *kbuf, int ksiz, int *vsiz_p) {
      return static_cast<char *>(tcbdbget(bdb, kbuf, ksiz, vsiz_p));
    }

    DEFINE_SYNC2(Get)
    DEFINE_ASYNC2(Get)

    TCLIST * Getlist(char *kbuf, int ksiz) {
      return tcbdbget4(bdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Getlist)
    DEFINE_ASYNC2(Getlist)

    int Vnum(char *kbuf, int ksiz) {
      return tcbdbvnum(bdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Vnum)
    DEFINE_ASYNC2(Vnum)

    int Vsiz(char *kbuf, int ksiz) {
      return tcbdbvsiz(bdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Vsiz)
    DEFINE_ASYNC2(Vsiz)

    TCLIST * Range(char *bkbuf, int bksiz, bool binc, char *ekbuf, int eksiz, 
                                                      bool einc, int max) {
      return tcbdbrange(bdb, bkbuf, bksiz, binc, ekbuf, eksiz, einc, max);
    }

    class RangeData : public virtual ArgsData {
      protected:
        String::Utf8Value bkbuf;
        int bksiz;
        bool binc;
        String::Utf8Value ekbuf;
        int eksiz;
        bool einc;
        int max;
        TCLIST *list;

      public:
        static bool
        checkArgs (const Arguments& args) {
          return (NOU(ARG1) || ARG1->IsBoolean()) &&
                 (NOU(ARG3) || ARG3->IsBoolean()) &&
                 (NOU(ARG4) || ARG4->IsNumber());
        }

        RangeData (const Arguments& args) 
            : bkbuf(ARG0), ekbuf(ARG2), ArgsData(args) {
          bksiz = ARG0->IsNull() ? -1 : bkbuf.length();
          binc = ARG1->BooleanValue();
          eksiz = ARG2->IsNull() ? -1 : ekbuf.length();
          einc = ARG3->BooleanValue();
          max = NOU(ARG4) ? -1 : ARG4->Int32Value();
        }

        ~RangeData () {
          tclistdel(list);
        }

        bool
        run () {
          list = tcw->Range(bksiz == -1 ? NULL : *bkbuf, bksiz, binc,
                           eksiz == -1 ? NULL : *ekbuf, eksiz, einc, max);
          return true;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(tclisttoary(list));
        }
    };

    class RangeAsyncData : public RangeData, public AsyncData {
      public:
        RangeAsyncData (const Arguments& args)
          : RangeData(args), AsyncData(args[5]), ArgsData(args) {}
    };

    DEFINE_SYNC2(Range)
    DEFINE_ASYNC2(Range)

    TCLIST * Fwmkeys(char *kbuf, int ksiz, int max) {
      return tcbdbfwmkeys(bdb, kbuf, ksiz, max);
    }

    DEFINE_SYNC2(Fwmkeys)
    DEFINE_ASYNC2(Fwmkeys)

    int Addint(char *kbuf, int ksiz, int num) {
      return tcbdbaddint(bdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Addint)
    DEFINE_ASYNC2(Addint)

    double Adddouble(char *kbuf, int ksiz, double num) {
      return tcbdbadddouble(bdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Adddouble)
    DEFINE_ASYNC2(Adddouble)

    bool Sync () {
      return tcbdbsync(bdb);
    }

    DEFINE_SYNC(Sync)
    DEFINE_ASYNC(Sync)

    bool Optimize (int32_t lmemb, int32_t nmemb, int64_t bnum, int8_t apow, 
                                                int8_t fpow, uint8_t opts) {
      return tcbdboptimize(bdb, lmemb, nmemb, bnum, apow, fpow, opts);
    }

    class OptimizeData : public TuneData {
      public:
        OptimizeData (const Arguments& args) : TuneData(args) {}

        bool run () {
          return tcw->Optimize(lmemb, nmemb, bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Optimize)

    class OptimizeAsyncData : public OptimizeData, public AsyncData {
      public:
        OptimizeAsyncData (const Arguments& args)
          : OptimizeData(args), AsyncData(ARG6) {}
    };

    DEFINE_ASYNC(Optimize)

    bool Vanish () {
      return tcbdbvanish(bdb);
    }

    DEFINE_SYNC(Vanish)
    DEFINE_ASYNC(Vanish)

    bool Copy (char *path) {
      return tcbdbcopy(bdb, path);
    }

    DEFINE_SYNC(Copy)
    DEFINE_ASYNC(Copy)

    bool Tranbegin () {
      return tcbdbtranbegin(bdb);
    }

    DEFINE_SYNC(Tranbegin)
    DEFINE_ASYNC(Tranbegin)

    bool Trancommit () {
      return tcbdbtrancommit(bdb);
    }

    DEFINE_SYNC(Trancommit)
    DEFINE_ASYNC(Trancommit)

    bool Tranabort () {
      return tcbdbtranabort(bdb);
    }

    DEFINE_SYNC(Tranabort)
    DEFINE_ASYNC(Tranabort)

    const char * Path () {
      return tcbdbpath(bdb);
    }

    DEFINE_SYNC2(Path)

    uint64_t Rnum () {
      return tcbdbrnum(bdb);
    }

    DEFINE_SYNC2(Rnum)

    uint64_t Fsiz () {
      return tcbdbfsiz(bdb);
    }

    DEFINE_SYNC2(Fsiz)
};

const Persistent<FunctionTemplate> BDB::Tmpl =
  Persistent<FunctionTemplate>::New(FunctionTemplate::New(BDB::New));

class CUR : TCWrap {
  public:
    CUR (TCBDB *bdb) {
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

      NODE_SET_PROTOTYPE_METHOD(tmpl, "first", FirstSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "firstAsync", FirstAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "last", LastSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "lastAsync", LastAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "jump", JumpSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "jumpAsync", JumpAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "prev", PrevSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "prevAsync", PrevAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "next", NextSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "nextAsync", NextAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", PutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", OutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "key", KeySync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "keyAsync", KeyAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "val", ValSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "valAsync", ValAsync);

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
      TCBDB *bdb = ObjectWrap::Unwrap<BDB>(Local<Object>::Cast(ARG0))->bdb;
      (new CUR(bdb))->Wrap(THIS);
      return THIS;
    }

    int Ecode () {
      return tcbdbecode(cur->bdb);
    }

    bool First () {
      return tcbdbcurfirst(cur);
    }

    class FirstData : public virtual ArgsData {
      public:
        FirstData(const Arguments& args) : ArgsData() {}

        bool run () {
          return tcw->First();
        }
    };

    DEFINE_SYNC(First)

    class FirstAsyncData : public FirstData, public AsyncData {
      public:
        FirstAsyncData(const Arguments& args)
          : AsyncData(args[0]), FirstData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(First)

    bool Last () {
      return tcbdbcurlast(cur);
    }

    class LastData : public virtual ArgsData {
      public:
        LastData(const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Last();
        }
    };

    DEFINE_SYNC(Last)

    class LastAsyncData : public LastData, public AsyncData {
      public:
        LastAsyncData(const Arguments& args)
          : AsyncData(args[0]), LastData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Last)

    bool Jump (char *kbuf, int ksiz) {
      return tcbdbcurjump(cur, kbuf, ksiz);
    }

    class JumpData : public KeyData {
      public:
        JumpData(const Arguments& args) : KeyData(args) {}

        bool run () {
          return tcw->Jump(*kbuf, ksiz);
        }
    };

    DEFINE_SYNC(Jump)

    class JumpAsyncData : public JumpData, public AsyncData {
      public:
        JumpAsyncData(const Arguments& args)
          : AsyncData(args[1]), JumpData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Jump)

    bool Prev () {
      return tcbdbcurprev(cur);
    }

    class PrevData : public virtual ArgsData {
      public:
        PrevData(const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Prev();
        }
    };

    DEFINE_SYNC(Prev)

    class PrevAsyncData : public PrevData, public AsyncData {
      public:
        PrevAsyncData(const Arguments& args)
          : AsyncData(args[0]), PrevData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Prev)

    bool Next () {
      return tcbdbcurnext(cur);
    }

    class NextData : public virtual ArgsData {
      public:
        NextData(const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Next();
        }
    };

    DEFINE_SYNC(Next)

    class NextAsyncData : public NextData, public AsyncData {
      public:
        NextAsyncData(const Arguments& args)
          : AsyncData(args[0]), NextData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Next)

    bool Put (char *kbuf, int ksiz, int cpmode) {
      return tcbdbcurput(cur, kbuf, ksiz, cpmode);
    }

    class PutData : public KeyData {
      private:
        int cpmode;

      public:
        PutData(const Arguments& args) : KeyData(args) {
          cpmode = NOU(args[1]) ?
            BDBCPCURRENT : args[1]->Int32Value();
        }

        static bool checkArgs (const Arguments& args) {
          return NOU(args[1]) || args[1]->IsNumber();
        }

        bool run () {
          return tcw->Put(*kbuf, ksiz, cpmode);
        }
    };

    DEFINE_SYNC(Put)

    class PutAsyncData : public PutData, public AsyncData {
      public:
        PutAsyncData(const Arguments& args)
          : AsyncData(args[2]), PutData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Put)

    class OutData : public virtual ArgsData {
      public:
        OutData(const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Out();
        }
    };

    bool Out () {
      return tcbdbcurout(cur);
    }

    DEFINE_SYNC(Out)

    class OutAsyncData : public OutData, public AsyncData {
      public:
        OutAsyncData(const Arguments& args)
          : AsyncData(args[0]), OutData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Out)

    char * Key (int *vsiz_p) {
      return static_cast<char *>(tcbdbcurkey(cur, vsiz_p));
    }

    class KeyData : public ValueData {
      public:
        KeyData(const Arguments& args) {}

        bool run () {
          vbuf = tcw->Key(&vsiz);
          return vbuf != NULL;
        }
    };

    DEFINE_SYNC2(Key)

    class KeyAsyncData : public KeyData, public AsyncData {
      public:
        KeyAsyncData(const Arguments& args)
          : AsyncData(args[0]), KeyData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC2(Key)

    char * Val (int *vsiz_p) {
      return static_cast<char *>(tcbdbcurval(cur, vsiz_p));
    }

    class ValData : public ValueData {
      public:
        ValData(const Arguments& args) {}

        bool run () {
          vbuf = tcw->Val(&vsiz);
          return vbuf != NULL;
        }
    };

    DEFINE_SYNC2(Val)

    class ValAsyncData : public ValData, public AsyncData {
      public:
        ValAsyncData(const Arguments& args)
          : AsyncData(args[0]), ValData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC2(Val)
};

class FDB : public TCWrap {
  public:
    FDB () {
      fdb = tcfdbnew();
    }

    ~FDB () {
      tcfdbdel(fdb);
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Local<FunctionTemplate> tmpl = FunctionTemplate::New(New);
      tmpl->InstanceTemplate()->SetInternalFieldCount(1);
      set_ecodes(tmpl);

      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OREADER);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OWRITER);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OCREAT);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OTRUNC);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, ONOLCK);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OLCKNB);
      DEFINE_PREFIXED_CONSTANT(tmpl, FDB, OTSYNC);

      NODE_SET_PROTOTYPE_METHOD(tmpl, "errmsg", ErrmsgSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "ecode", EcodeSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "setmutex", SetmutexSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tune", TuneSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "open", OpenSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "openAsync", OpenAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "close", CloseSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "closeAsync", CloseAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "put", PutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeep", PutkeepSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putkeepAsync", PutkeepAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcat", PutcatSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "putcatAsync", PutcatAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "out", OutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "get", GetSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "getAsync", GetAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsiz", VsizSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vsizAsync", VsizAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinit", IterinitSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iterinitAsync", IterinitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternext", IternextSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "iternextAsync", IternextAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "range", RangeSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rangeAsync", RangeAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addint", AddintSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "addintAsync", AddintAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddouble", AdddoubleSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "adddoubleAsync", AdddoubleAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "sync", SyncSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "syncAsync", SyncAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimize", OptimizeSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "optimizeAsync", OptimizeAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanish", VanishSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "vanishAsync", VanishAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copy", CopySync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "copyAsync", CopyAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbegin", TranbeginSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranbeginAsync", TranbeginAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommit", TrancommitSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "trancommitAsync", TrancommitAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabort", TranabortSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "tranabortAsync", TranabortAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "path", PathSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "rnum", RnumSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "fsiz", FsizSync);

      target->Set(String::New("FDB"), tmpl->GetFunction());
    }

  private:
    TCFDB *fdb;

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (!args.IsConstructCall()) return args.Callee()->NewInstance();
      (new FDB)->Wrap(THIS);
      return THIS;
    }

    int Ecode () {
      return tcfdbecode(fdb);
    }

    DEFINE_SYNC2(Ecode)

    const char * Errmsg (int ecode) {
      return tcfdberrmsg(ecode);
    }

    DEFINE_SYNC2(Errmsg)

    bool Setmutex () {
      return tcfdbsetmutex(fdb);
    }

    DEFINE_SYNC(Setmutex)

    bool Tune (int32_t width, int64_t limsiz) {
      return tcfdbtune(fdb, width, limsiz);
    }

    class TuneData : public virtual ArgsData {
      protected:
        int32_t width;
        int64_t limsiz;

      public:
        static bool checkArgs (const Arguments& args) {
          return (ARG0->IsNumber() || NOU(ARG0)) &&
                 (ARG1->IsNumber() || NOU(ARG1));
        }

        TuneData (const Arguments& args) : ArgsData(args) {
          width = NOU(ARG0) ? -1 : ARG0->Int32Value();
          limsiz = NOU(ARG1) ? -1 : ARG1->IntegerValue();
        }

        bool run () {
          return tcw->Tune(width, limsiz);
        }
    };

    DEFINE_SYNC(Tune)

    bool Open (char *path, int omode) {
      return tcfdbopen(fdb, path, omode);
    }

    class OpenData : public FilenameData {
      protected:
        int omode;

      public:
        OpenData (const Arguments& args) : FilenameData(args) {
          omode = NOU(ARG1) ? FDBOREADER : ARG1->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return FilenameData::checkArgs(args) &&
            (NOU(ARG1) || ARG1->IsNumber());
        }

        bool
        run () {
          return tcw->Open(*path, omode);
        }
    };

    DEFINE_SYNC(Open)

    class OpenAsyncData : public OpenData, public AsyncData {
      public:
        OpenAsyncData (const Arguments& args)
          : OpenData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    DEFINE_ASYNC(Open)

    bool Close () {
      return tcfdbclose(fdb);
    }

    DEFINE_SYNC(Close)
    DEFINE_ASYNC(Close)

    bool Put(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcfdbput2(fdb, kbuf, ksiz, vbuf, vsiz);
    }



    DEFINE_SYNC(Put)
    DEFINE_ASYNC(Put)

    bool Putkeep(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcfdbputkeep2(fdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putkeep)
    DEFINE_ASYNC(Putkeep)

    bool Putcat(char *kbuf, int ksiz, char *vbuf, int vsiz) {
      return tcfdbputcat2(fdb, kbuf, ksiz, vbuf, vsiz);
    }

    DEFINE_SYNC(Putcat)
    DEFINE_ASYNC(Putcat)

    bool Out(char *kbuf, int ksiz) {
      return tcfdbout2(fdb, kbuf, ksiz);
    }

    DEFINE_SYNC(Out)
    DEFINE_ASYNC(Out)

    char * Get(char *kbuf, int ksiz, int *vsiz_p) {
      return static_cast<char *>(tcfdbget2(fdb, kbuf, ksiz, vsiz_p));
    }

    DEFINE_SYNC2(Get)
    DEFINE_ASYNC2(Get)

    int Vsiz(char *kbuf, int ksiz) {
      return tcfdbvsiz2(fdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Vsiz)
    DEFINE_ASYNC2(Vsiz)

    bool Iterinit () {
      return tcfdbiterinit(fdb);
    }

    DEFINE_SYNC(Iterinit)
    DEFINE_ASYNC(Iterinit)

    char * Iternext (int *vsiz_p) {
      return static_cast<char *>(tcfdbiternext2(fdb, vsiz_p));
    }

    DEFINE_SYNC2(Iternext)
    DEFINE_ASYNC2(Iternext)

    TCLIST * Range(char *ibuf, int isiz, int max) {
      return tcfdbrange4(fdb, ibuf, isiz, max);
    }

    // Get keys with an interval notation
    // arg[0] : interval notation
    // arg[1] : maximum number of keys to be fetched
    class RangeData : public GetlistData {
      protected:
        int max;

      public:
        static bool
        checkArgs (const Arguments& args) {
          return NOU(ARG1) || ARG1->IsNumber();
        }

        RangeData (const Arguments& args) : GetlistData(args) {
          max = NOU(ARG1) ? -1 : ARG1->Int32Value();
        }

        bool
        run () {
          list = tcw->Range(*kbuf, ksiz, max);
          return true;
        }
    };

    class RangeAsyncData : public RangeData, public AsyncData {
      public:
        RangeAsyncData (const Arguments& args)
          : RangeData(args), AsyncData(args[5]), ArgsData(args) {}
    };

    DEFINE_SYNC2(Range)
    DEFINE_ASYNC2(Range)

    int Addint(char *kbuf, int ksiz, int num) {
      return tcfdbaddint(fdb, tcfdbkeytoid(kbuf, ksiz), num);
    }

    DEFINE_SYNC2(Addint)
    DEFINE_ASYNC2(Addint)

    double Adddouble(char *kbuf, int ksiz, double num) {
      return tcfdbadddouble(fdb, tcfdbkeytoid(kbuf, ksiz), num);
    }

    DEFINE_SYNC2(Adddouble)
    DEFINE_ASYNC2(Adddouble)

    bool Sync () {
      return tcfdbsync(fdb);
    }

    DEFINE_SYNC(Sync)
    DEFINE_ASYNC(Sync)

    bool Optimize (int32_t width, int64_t limsiz) {
      return tcfdboptimize(fdb, width, limsiz);
    }

    class OptimizeData : public TuneData {
      public:
        OptimizeData (const Arguments& args) : TuneData(args) {}

        bool run () {
          return tcw->Optimize(width, limsiz);
        }
    };

    DEFINE_SYNC(Optimize)

    class OptimizeAsyncData : public OptimizeData, public AsyncData {
      public:
        OptimizeAsyncData (const Arguments& args)
          : OptimizeData(args), AsyncData(ARG2) {}
    };

    DEFINE_ASYNC(Optimize)

    bool Vanish () {
      return tcfdbvanish(fdb);
    }

    DEFINE_SYNC(Vanish)
    DEFINE_ASYNC(Vanish)

    bool Copy (char *path) {
      return tcfdbcopy(fdb, path);
    }

    DEFINE_SYNC(Copy)
    DEFINE_ASYNC(Copy)

    bool Tranbegin () {
      return tcfdbtranbegin(fdb);
    }

    DEFINE_SYNC(Tranbegin)
    DEFINE_ASYNC(Tranbegin)

    bool Trancommit () {
      return tcfdbtrancommit(fdb);
    }

    DEFINE_SYNC(Trancommit)
    DEFINE_ASYNC(Trancommit)

    bool Tranabort () {
      return tcfdbtranabort(fdb);
    }

    DEFINE_SYNC(Tranabort)
    DEFINE_ASYNC(Tranabort)

    const char * Path () {
      return tcfdbpath(fdb);
    }

    DEFINE_SYNC2(Path)

    uint64_t Rnum () {
      return tcfdbrnum(fdb);
    }

    DEFINE_SYNC2(Rnum)

    uint64_t Fsiz () {
      return tcfdbfsiz(fdb);
    }

    DEFINE_SYNC2(Fsiz)
};

class TDB : public TCWrap {
  public:
    TCTDB *tdb;

    const static Persistent<FunctionTemplate> Tmpl;

    TDB () {
      tdb = tctdbnew();
    }

    ~TDB () {
      tctdbdel(tdb);
    }

    static void
    Initialize (const Handle<Object> target) {
      HandleScope scope;
      Tmpl->InstanceTemplate()->SetInternalFieldCount(1);
      set_ecodes(Tmpl);

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

      NODE_SET_PROTOTYPE_METHOD(Tmpl, "errmsg", ErrmsgSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "ecode", EcodeSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setmutex", SetmutexSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tune", TuneSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setcache", SetcacheSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setxmsiz", SetxmsizSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setdfunit", SetdfunitSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "open", OpenSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "openAsync", OpenAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "close", CloseSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "closeAsync", CloseAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "put", PutSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putAsync", PutAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeep", PutkeepSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putkeepAsync", PutkeepAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcat", PutcatSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "putcatAsync", PutcatAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "out", OutSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "outAsync", OutAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "get", GetSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "getAsync", GetAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsiz", VsizSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vsizAsync", VsizAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iterinit", IterinitSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iterinitAsync", IterinitAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iternext", IternextSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "iternextAsync", IternextAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeys", FwmkeysSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fwmkeysAsync", FwmkeysAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addint", AddintSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "addintAsync", AddintAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddouble", AdddoubleSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "adddoubleAsync", AdddoubleAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "sync", SyncSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "syncAsync", SyncAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimize", OptimizeSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "optimizeAsync", OptimizeAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanish", VanishSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "vanishAsync", VanishAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copy", CopySync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "copyAsync", CopyAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbegin", TranbeginSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranbeginAsync", TranbeginAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommit", TrancommitSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "trancommitAsync", TrancommitAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabort", TranabortSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "tranabortAsync", TranabortAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "path", PathSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "rnum", RnumSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "fsiz", FsizSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setindex", SetindexSync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "setindexAsync", SetindexAsync);
      NODE_SET_PROTOTYPE_METHOD(Tmpl, "genuid", Genuid);

      target->Set(String::New("TDB"), Tmpl->GetFunction());
    }

  private:

    static Handle<Value>
    New (const Arguments& args) {
      HandleScope scope;
      if (!args.IsConstructCall()) return args.Callee()->NewInstance();
      (new TDB)->Wrap(THIS);
      return THIS;
    }

    int Ecode () {
      return tctdbecode(tdb);
    }

    DEFINE_SYNC2(Ecode)

    const char * Errmsg (int ecode) {
      return tctdberrmsg(ecode);
    }

    DEFINE_SYNC2(Errmsg)

    bool Setmutex () {
      return tctdbsetmutex(tdb);
    }

    DEFINE_SYNC(Setmutex)

    bool Tune (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) {
      return tctdbtune(tdb, bnum, apow, fpow, opts);
    }

    class TuneData : public virtual ArgsData {
      protected:
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

        TuneData (const Arguments& args) : ArgsData(args) {
          bnum = NOU(ARG0) ? -1 : ARG0->IntegerValue();
          apow = NOU(ARG1) ? -1 : ARG1->Int32Value();
          fpow = NOU(ARG2) ? -1 : ARG2->Int32Value();
          opts = NOU(ARG3) ? 0 : ARG3->Int32Value();
        }

        bool run () {
          return tcw->Tune(bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Tune)

    bool Setcache (int32_t rcnum, int32_t lcnum, int32_t ncnum) {
      return tctdbsetcache(tdb, rcnum, lcnum, ncnum);
    }

    class SetcacheData : public ArgsData {
      private:
        int32_t rcnum;
        int32_t lcnum;
        int32_t ncnum;

      public:
        SetcacheData (const Arguments& args) : ArgsData(args) {
          rcnum = NOU(ARG0) ? -1 : ARG0->Int32Value();
          lcnum = NOU(ARG1) ? -1 : ARG0->Int32Value();
          ncnum = NOU(ARG2) ? -1 : ARG0->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return (NOU(ARG0) || ARG0->IsNumber()) &&
                 (NOU(ARG1) || ARG1->IsNumber()) &&
                 (NOU(ARG2) || ARG2->IsNumber());
        }

        bool
        run () {
          return tcw->Setcache(rcnum, lcnum, ncnum);
        }
    };

    DEFINE_SYNC(Setcache)

    bool Setxmsiz (int64_t xmsiz) {
      return tctdbsetxmsiz(tdb, xmsiz);
    }

    DEFINE_SYNC(Setxmsiz)

    bool Setdfunit (int32_t dfunit) {
      return tctdbsetdfunit(tdb, dfunit);
    }

    DEFINE_SYNC(Setdfunit)

    bool Open (char *path, int omode) {
      return tctdbopen(tdb, path, omode);
    }

    class OpenData : public FilenameData {
      protected:
        int omode;

      public:
        OpenData (const Arguments& args) : FilenameData(args) {
          omode = NOU(ARG1) ? TDBOREADER : ARG1->Int32Value();
        }

        static bool
        checkArgs (const Arguments& args) {
          return FilenameData::checkArgs(args) &&
            (NOU(ARG1) || ARG1->IsNumber());
        }

        bool
        run () {
          return tcw->Open(*path, omode);
        }
    };

    DEFINE_SYNC(Open)

    class OpenAsyncData : public OpenData, public AsyncData {
      public:
        OpenAsyncData (const Arguments& args)
          : OpenData(args), AsyncData(args[2]), ArgsData(args) {}
    };

    DEFINE_ASYNC(Open)

    bool Close () {
      return tctdbclose(tdb);
    }

    DEFINE_SYNC(Close)
    DEFINE_ASYNC(Close)

    bool Put(char *kbuf, int ksiz, TCMAP *map) {
      return tctdbput(tdb, kbuf, ksiz, map);
    }

    class PutData : public KeyData {
      protected:
        TCMAP *map;

      public:
        PutData (const Arguments& args) : KeyData(args) {
          map = objtotcmap(Local<Object>::Cast(args[1]));
        }

        ~PutData () {
          tcmapdel(map);
        }

        static bool checkArgs (const Arguments& args) {
          return ARG1->IsObject();
        }

        bool run () {
          return tcw->Put(*kbuf, ksiz, map);
        }
    };

    DEFINE_SYNC(Put)

    class PutAsyncData : public PutData, public AsyncData {
      public:
        PutAsyncData (const Arguments& args)
          : PutData(args), AsyncData(ARG2), ArgsData(args) {}
    };

    DEFINE_ASYNC(Put)

    bool Putkeep(char *kbuf, int ksiz, TCMAP *map) {
      return tctdbputkeep(tdb, kbuf, ksiz, map);
    }

    class PutkeepData : public PutData {
      public:
        PutkeepData (const Arguments& args) : PutData(args) {}

        bool run () {
          return tcw->Putkeep(*kbuf, ksiz, map);
        }
    };

    DEFINE_SYNC(Putkeep)

    class PutkeepAsyncData : public PutkeepData, public AsyncData {
      public:
        PutkeepAsyncData (const Arguments& args)
          : PutkeepData(args), AsyncData(ARG2), ArgsData(args) {}
    };

    DEFINE_ASYNC(Putkeep)

    bool Putcat(char *kbuf, int ksiz, TCMAP *map) {
      return tctdbputcat(tdb, kbuf, ksiz, map);
    }

    class PutcatData : public PutData {
      public:
        PutcatData (const Arguments& args) : PutData(args) {}

        bool run () {
          return tcw->Putcat(*kbuf, ksiz, map);
        }
    };

    DEFINE_SYNC(Putcat)

    class PutcatAsyncData : public PutcatData, public AsyncData {
      public:
        PutcatAsyncData (const Arguments& args)
          : PutcatData(args), AsyncData(ARG2), ArgsData(args) {}
    };

    DEFINE_ASYNC(Putcat)

    bool Out(char *kbuf, int ksiz) {
      return tctdbout(tdb, kbuf, ksiz);
    }

    DEFINE_SYNC(Out)
    DEFINE_ASYNC(Out)

    TCMAP * Get(char *kbuf, int ksiz) {
      return tctdbget(tdb, kbuf, ksiz);
    }

    class GetData : public KeyData {
      protected:
        TCMAP *map;

      public:
        GetData (const Arguments& args) : KeyData(args) {}

        ~GetData () {
          tcmapdel(map);
        }

        bool run () {
          map = tcw->Get(*kbuf, ksiz);
          return map != NULL;
        }

        Handle<Value>
        returnValue () {
          HandleScope scope;
          return scope.Close(tcmaptoobj(map));
        }
    };

    DEFINE_SYNC2(Get)

    class GetAsyncData : public GetData, public AsyncData {
      public:
        GetAsyncData (const Arguments& args)
          : GetData(args), AsyncData(ARG1), ArgsData(args) {}
    };

    DEFINE_ASYNC2(Get)

    int Vsiz(char *kbuf, int ksiz) {
      return tctdbvsiz(tdb, kbuf, ksiz);
    }

    DEFINE_SYNC2(Vsiz)
    DEFINE_ASYNC2(Vsiz)

    bool Iterinit () {
      return tctdbiterinit(tdb);
    }

    DEFINE_SYNC(Iterinit)
    DEFINE_ASYNC(Iterinit)

    char * Iternext (int *vsiz_p) {
      return static_cast<char *>(tctdbiternext(tdb, vsiz_p));
    }

    DEFINE_SYNC2(Iternext)
    DEFINE_ASYNC2(Iternext)

    TCLIST * Fwmkeys(char *kbuf, int ksiz, int max) {
      return tctdbfwmkeys(tdb, kbuf, ksiz, max);
    }

    DEFINE_SYNC2(Fwmkeys)
    DEFINE_ASYNC2(Fwmkeys)

    int Addint(char *kbuf, int ksiz, int num) {
      return tctdbaddint(tdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Addint)
    DEFINE_ASYNC2(Addint)

    double Adddouble(char *kbuf, int ksiz, double num) {
      return tctdbadddouble(tdb, kbuf, ksiz, num);
    }

    DEFINE_SYNC2(Adddouble)
    DEFINE_ASYNC2(Adddouble)

    bool Sync () {
      return tctdbsync(tdb);
    }

    DEFINE_SYNC(Sync)
    DEFINE_ASYNC(Sync)

    bool Optimize (int64_t bnum, int8_t apow, int8_t fpow, uint8_t opts) {
      return tctdboptimize(tdb, bnum, apow, fpow, opts);
    }

    class OptimizeData : public TuneData {
      public:
        OptimizeData (const Arguments& args) : TuneData(args) {}

        bool run () {
          return tcw->Optimize(bnum, apow, fpow, opts);
        }
    };

    DEFINE_SYNC(Optimize)

    class OptimizeAsyncData : public OptimizeData, public AsyncData {
      public:
        OptimizeAsyncData (const Arguments& args)
          : OptimizeData(args), AsyncData(ARG4) {}
    };

    DEFINE_ASYNC(Optimize)

    bool Vanish () {
      return tctdbvanish(tdb);
    }

    DEFINE_SYNC(Vanish)
    DEFINE_ASYNC(Vanish)

    bool Copy (char *path) {
      return tctdbcopy(tdb, path);
    }

    DEFINE_SYNC(Copy)
    DEFINE_ASYNC(Copy)

    bool Tranbegin () {
      return tctdbtranbegin(tdb);
    }

    DEFINE_SYNC(Tranbegin)
    DEFINE_ASYNC(Tranbegin)

    bool Trancommit () {
      return tctdbtrancommit(tdb);
    }

    DEFINE_SYNC(Trancommit)
    DEFINE_ASYNC(Trancommit)

    bool Tranabort () {
      return tctdbtranabort(tdb);
    }

    DEFINE_SYNC(Tranabort)
    DEFINE_ASYNC(Tranabort)

    const char * Path () {
      return tctdbpath(tdb);
    }

    DEFINE_SYNC2(Path)

    uint64_t Rnum () {
      return tctdbrnum(tdb);
    }

    DEFINE_SYNC2(Rnum)

    uint64_t Fsiz () {
      return tctdbfsiz(tdb);
    }

    DEFINE_SYNC2(Fsiz)

    bool Setindex (const char* name, int type) {
      return tctdbsetindex(tdb, name, type);
    }

    class SetindexData : public virtual ArgsData {
      private:
        String::Utf8Value name;
        int type;

      public:
        SetindexData (const Arguments& args) : name(ARG0), ArgsData(args) {
          type = ARG1->Int32Value();
        }

        static bool checkArgs (const Arguments& args) {
          return ARG0->IsString() && ARG1->IsNumber();
        }

        bool run () {
          return tcw->Setindex(*name, type);
        }
    };

    DEFINE_SYNC(Setindex)

    class SetindexAsyncData : public SetindexData, public AsyncData {
      public:
        SetindexAsyncData (const Arguments& args)
          : AsyncData(ARG2), SetindexData(args), ArgsData(args) {}
    };

    DEFINE_ASYNC(Setindex)

    // bug: JavaScript can't handle integers greater than Math.pow(2,53)
    static Handle<Value>
    Genuid (const Arguments& args) {
      HandleScope scope;
      int64_t siz = tctdbgenuid(ObjectWrap::Unwrap<TDB>(THIS)->tdb);
      return Integer::New(siz);
    }
};

const Persistent<FunctionTemplate> TDB::Tmpl =
  Persistent<FunctionTemplate>::New(FunctionTemplate::New(TDB::New));

class QRY : TCWrap {
  public:
    QRY (TCTDB *db) {
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
      NODE_SET_PROTOTYPE_METHOD(tmpl, "search", SearchSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "searchAsync", SearchAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "searchout", SearchoutSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "searchoutAsync", SearchoutAsync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "hint", Hint);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "metasearch", MetasearchSync);
      NODE_SET_PROTOTYPE_METHOD(tmpl, "metasearchAsync", MetasearchAsync);

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
      TCTDB *db = ObjectWrap::Unwrap<TDB>(Local<Object>::Cast(ARG0))->tdb;
      (new QRY(db))->Wrap(THIS);
      return THIS;
    }

    static Handle<Value>
    Addcond (const Arguments& args) {
      HandleScope scope;
      if (!ARG1->IsNumber()) {
        return THROW_BAD_ARGS;
      }
      tctdbqryaddcond(
          Backend(THIS),
          *String::Utf8Value(ARG0),
          ARG1->Int32Value(),
          *String::Utf8Value(ARG2));
      return Undefined();
    }

    static Handle<Value>
    Setorder (const Arguments& args) {
      HandleScope scope;
      if (!(ARG1->IsNumber() || NOU(ARG1))) {
        return THROW_BAD_ARGS;
      }
      tctdbqrysetorder(
          Backend(THIS),
          *String::Utf8Value(ARG0),
          NOU(ARG1) ? TDBQOSTRASC : ARG1->Int32Value());
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
          NOU(ARG1) ? -1 : ARG0->Int32Value(),
          NOU(ARG1) ? -1 : ARG1->Int32Value());
      return Undefined();
    }

    TCLIST * Search () {
      return tctdbqrysearch(qry);
    }

    class SearchData : public virtual ArgsData {
      private:
        TCLIST *list;

      public:
        SearchData (const Arguments& args) : ArgsData(args) {}

        ~SearchData () {
          tclistdel(list);
        }

        bool run () {
          list = tcw->Search();
          return true;
        }

        Handle<Value> returnValue () {
          HandleScope scope;
          return scope.Close(tclisttoary(list));
        }
    };

    DEFINE_SYNC2(Search)

    class SearchAsyncData : public SearchData, public AsyncData {
      public:
        SearchAsyncData (const Arguments& args)
          : SearchData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    DEFINE_ASYNC2(Search)

    bool Searchout () {
      return tctdbqrysearchout(qry);
    }

    class SearchoutData : public virtual ArgsData {
      public:
        SearchoutData (const Arguments& args) : ArgsData(args) {}

        bool run () {
          return tcw->Searchout();
        }
    };

    DEFINE_SYNC(Searchout)

    class SearchoutAsyncData : public SearchoutData, public AsyncData {
      public:
        SearchoutAsyncData (const Arguments& args)
          : SearchoutData(args), AsyncData(ARG0), ArgsData(args) {}
    };

    DEFINE_ASYNC(Searchout)

    static Handle<Value>
    Hint (const Arguments& args) {
      HandleScope scope;
      const char *hint = tctdbqryhint(Backend(THIS));
      return String::New(hint);
    }

    TCLIST * Metasearch (TDBQRY **qrys, int num, int type) {
      return tctdbmetasearch(qrys, num, type);
    }

    class MetasearchData : public virtual ArgsData {
      private:
        TDBQRY **qrys;
        int qnum;
        int type;
        TCLIST *list;

      public:
        MetasearchData (const Arguments& args) : ArgsData(args) {
          Local<Array> others = Local<Array>::Cast(ARG0);
          int num = others->Length();
          qrys = static_cast<TDBQRY **>(tcmalloc(sizeof(*qrys) * (num+1)));
          qnum = 0;
          qrys[qnum++] = Backend(THIS);
          Local<Object> oqry;
          for (int i = 0; i < num; i++) {
            oqry = others->CloneElementAt(i);
            if (TDB::Tmpl->HasInstance(oqry)) {
              qrys[qnum++] = Backend(oqry);
            }
          }
          type = NOU(ARG1) ? TDBMSUNION : ARG1->Int32Value();
        }

        ~MetasearchData () {
          tcfree(qrys);
          tclistdel(list);
        }

        bool run () {
          list = tcw->Metasearch(qrys, qnum, type);
          return true;
        }

        static bool checkArgs (const Arguments& args) {
          return ARG0->IsArray() &&
                 (ARG1->IsNumber() || NOU(ARG1));
        }

        Handle<Value> returnValue () {
          HandleScope scope;
          return scope.Close(tclisttoary(list));
        }
    };

    DEFINE_SYNC2(Metasearch)

    class MetasearchAsyncData : public MetasearchData, public AsyncData {
      public:
        MetasearchAsyncData (const Arguments& args)
          : MetasearchData(args), AsyncData(ARG2), ArgsData(args) {}
    };

    DEFINE_ASYNC2(Metasearch)
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

