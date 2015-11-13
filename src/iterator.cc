/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#include <node.h>
#include <node_buffer.h>
#include <string.h>
#include <nan.h>
 
#include "database.h"
#include "iterator.h"
#include "iterator_async.h"

namespace nlmdb {

inline int compare(std::string *str, MDB_val *value) {
  const int min_len = (str->length() < value->mv_size)
      ? str->length()
      : value->mv_size;
  int r = memcmp(str->c_str(), (char *)value->mv_data, min_len);
  if (r == 0) {
    if (str->length() < value->mv_size) r = -1;
    else if (str->length() > value->mv_size) r = +1;
  }
  return r;
}

Iterator::Iterator (
    Database    *database
  , uint32_t     id
  , std::string *start
  , std::string *end
  , bool         reverse
  , bool         keys
  , bool         values
  , int          limit
  , bool         keyAsBuffer
  , bool         valueAsBuffer
  , bool         startIsExclusive
  , bool         endIsExclusive
) : database(database)
  , id(id)
  , start(start)
  , end(end)
  , reverse(reverse)
  , keys(keys)
  , values(values)
  , limit(limit)
  , keyAsBuffer(keyAsBuffer)
  , valueAsBuffer(valueAsBuffer)
  , startIsExclusive(startIsExclusive)
  , endIsExclusive(endIsExclusive)
{
  count     = 0;
  started   = false;
  nexting   = false;
  ended     = false;
  endWorker = NULL;
};

Iterator::~Iterator () {
  if (start != NULL)
    delete start;
  if (end != NULL)
    delete end;
};

int Iterator::Current (MDB_val *key, MDB_val *value) {

  int rc = -1;
  if (started) {
    rc = mdb_cursor_get(cursor, key, value, MDB_GET_CURRENT);
  }
  return rc;
}

int Iterator::Next (MDB_val *key, MDB_val *value) {
  //std::cerr << "Iterator::Next " << started << ", " << id << std::endl;
  int rc = 0;

  if (!started) {
    //std::cerr << "opening cursor... " << std::endl;
    rc = database->NewIterator(&txn, &cursor);
    //std::cerr << "opened cursor!! " << cursor << ", " << strerror(rc) << std::endl;
    if (rc) {
      //std::cerr << "returning 0: " << rc << std::endl;
      return rc;
    }

    if (start != NULL) {
      key->mv_data = (void*)start->data();
      key->mv_size = start->length();
      rc = mdb_cursor_get(cursor, key, value, MDB_SET_RANGE);
      
      if (rc == MDB_NOTFOUND) {
        rc = mdb_cursor_get(cursor, key, value, reverse ? MDB_LAST : MDB_FIRST);
      } else if (rc == 0) {
        // when iterating in reverse:
        //   - 'lt'  always backs up one key
        //   - 'lte' backs up if the current key isn't equal to the start key
        if (reverse && (startIsExclusive || compare(start, key) != 0)) {
          rc = mdb_cursor_get(cursor, key, value, MDB_PREV);
        }
        // when iterating forward:
        //   - 'gt' advances one key if the current key is equal to the start key
        else if (!reverse && startIsExclusive && compare(start, key) == 0) {
          rc = mdb_cursor_get(cursor, key, value, MDB_NEXT);
        }
      }
    } else if (reverse) {
      rc = mdb_cursor_get(cursor, key, value, MDB_LAST);
    } else {
      rc = mdb_cursor_get(cursor, key, value, MDB_FIRST);
    }

    started = true;
    //std::cerr << "Started " << started << std::endl;
  } else {
    //std::cerr << "started! getting cursor..." << std::endl;
    if (reverse)
      rc = mdb_cursor_get(cursor, key, value, MDB_PREV);
    else
      rc = mdb_cursor_get(cursor, key, value, MDB_NEXT);
    //std::cerr << "started! got cursor..." << std::endl;
  }

  if (rc) {
    //std::cerr << "returning 1: " << rc << std::endl;
    return rc;
  }

  //std::cerr << "***" << std::string((const char*)key->mv_data, key->mv_size) << std::endl;
  //if (end != NULL)
    //std::cerr << "***end=" << end->c_str() << ", " << reverse << ", " << compare(end, key) << std::endl;

  if ((limit < 0 || ++count <= limit)
      && (end == NULL
          || (reverse && compare(end, key) < (endIsExclusive ? 0 : 1))
          || (!reverse && compare(end, key) > (endIsExclusive ? 0 : -1)))) {
    return 0; // good to continue
  }

  key = 0;
  value = 0;
  return MDB_NOTFOUND;
}

void Iterator::End () {
  //std::cerr << "Iterator::End " << started << ", " << id << std::endl;
  if (started) {
    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
  }
}

void Iterator::Release () {
  //std::cerr << "Iterator::Release " << started << ", " << id << std::endl;
  database->ReleaseIterator(id);
}

void checkEndCallback (Iterator* iterator) {
  iterator->nexting = false;
  if (iterator->endWorker != NULL) {
    Nan::AsyncQueueWorker(iterator->endWorker);
    iterator->endWorker = NULL;
  }
}

NAN_METHOD(Iterator::Next) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());

  if (info.Length() == 0 || !info[0]->IsFunction()) {
    return Nan::ThrowError("next() requires a callback argument");
  }

  v8::Local<v8::Function> callback = info[0].As<v8::Function>();

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() after end()")
  }

  if (iterator->nexting) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "cannot call next() before previous next() has completed")
  }

  NextWorker* worker = new NextWorker(
      iterator
    , new Nan::Callback(callback)
    , checkEndCallback
  );
  iterator->nexting = true;
  Nan::AsyncQueueWorker(worker);

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(Iterator::NextSync) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());

  if (iterator->ended) {
    return;
  }

  MDB_val key;
  MDB_val value;
  int rval = iterator->Next(&key, &value);

  if (rval) {
     return;
  }
  else {

    v8::Local<v8::Value> returnKey;
    if (iterator->keyAsBuffer) {
      returnKey = Nan::NewBuffer((char*)key.mv_data, key.mv_size).ToLocalChecked();
    } else {
      returnKey =  Nan::New<v8::String>((char*)key.mv_data, key.mv_size).ToLocalChecked();
    }

    info.GetReturnValue().Set(returnKey);
  }
}

NAN_METHOD(Iterator::KeySync) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());

  if (iterator->ended) {
    return;
  }

  MDB_val key;
  MDB_val value;
  int rval = iterator->Current(&key, &value);

  if (rval) {
     return;
  }
  else {

    v8::Local<v8::Value> returnKey;
    if (iterator->keyAsBuffer) {
      returnKey = Nan::NewBuffer((char*)key.mv_data, key.mv_size).ToLocalChecked();
    } else {
      returnKey =  Nan::New<v8::String>((char*)key.mv_data, key.mv_size).ToLocalChecked();
    }

    info.GetReturnValue().Set(returnKey);
  }
}

NAN_METHOD(Iterator::ValSync) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());

  if (iterator->ended) {
    return;
  }

  MDB_val key;
  MDB_val value;
  int rval = iterator->Current(&key, &value);

  if (rval) {
     return;
  }
  else {

    v8::Local<v8::Value> returnVal;
    if (iterator->valueAsBuffer) {
      returnVal = Nan::NewBuffer((char*)value.mv_data, value.mv_size).ToLocalChecked();
    } else {
      returnVal =  Nan::New<v8::String>((char*)value.mv_data, value.mv_size).ToLocalChecked();
    }

    info.GetReturnValue().Set(returnVal);
  }
}

NAN_METHOD(Iterator::End) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());
  //std::cerr << "Iterator::End" << iterator->id << ", " << iterator->nexting << ", " << iterator->ended << std::endl;

  if (info.Length() == 0 || !info[0]->IsFunction()) {
    return Nan::ThrowError("end() requires a callback argument");
  }

  v8::Local<v8::Function> callback = v8::Local<v8::Function>::Cast(info[0]);

  if (iterator->ended) {
    NL_RETURN_CALLBACK_OR_ERROR(callback, "end() already called on iterator")
  }

  EndWorker* worker = new EndWorker(
      iterator
    , new Nan::Callback(callback)
  );
  iterator->ended = true;

  if (iterator->nexting) {
    // waiting for a next() to return, queue the end
    //std::cerr << "Iterator is nexting: " << iterator->id << std::endl;
    iterator->endWorker = worker;
  } else {
    //std::cerr << "Iterator can be ended: " << iterator->id << std::endl;
    Nan::AsyncQueueWorker(worker);
  }

  info.GetReturnValue().Set(info.Holder());
}

NAN_METHOD(Iterator::EndSync) {
  Nan::HandleScope scope;

  Iterator* iterator = Nan::ObjectWrap::Unwrap<Iterator>(info.This());
  //std::cerr << "Iterator::End" << iterator->id << ", " << iterator->nexting << ", " << iterator->ended << std::endl;

  if (iterator->ended) {
    return;
  }

  iterator->ended = true;
  if (iterator->nexting) {
    printf("nlmdb::ERROR: EndSync - still nexting\n");
    return;
  }

  iterator->End();
  iterator->Release();

  return;
}

static Nan::Persistent<v8::FunctionTemplate> iterator_constructor;

void Iterator::Init () {

  v8::Local<v8::FunctionTemplate> tpl = Nan::New<v8::FunctionTemplate>(Iterator::New);
  iterator_constructor.Reset(tpl);
  tpl->SetClassName(Nan::New("Iterator").ToLocalChecked());
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  Nan::SetPrototypeMethod(tpl, "nextsync", Iterator::NextSync);
  Nan::SetPrototypeMethod(tpl, "keysync", Iterator::KeySync);
  Nan::SetPrototypeMethod(tpl, "valsync", Iterator::ValSync);
  Nan::SetPrototypeMethod(tpl, "endsync", Iterator::EndSync);

  Nan::SetPrototypeMethod(tpl, "next", Iterator::Next);
  Nan::SetPrototypeMethod(tpl, "end", Iterator::End);
}

v8::Local<v8::Object> Iterator::NewInstance (
        v8::Local<v8::Object> database
      , v8::Local<v8::Number> id
      , v8::Local<v8::Object> optionsObj
    ) {

  Nan::EscapableHandleScope scope;

  v8::Local<v8::Object> instance;

  v8::Local<v8::FunctionTemplate> constructorHandle =
      Nan::New(iterator_constructor);

  if (optionsObj.IsEmpty()) {
    v8::Local<v8::Value> argv[] = { database, id };
    instance = constructorHandle->GetFunction()->NewInstance(2, argv);
  } else {
    v8::Local<v8::Value> argv[] = { database, id, optionsObj };
    instance = constructorHandle->GetFunction()->NewInstance(3, argv);
  }

  return scope.Escape(instance);
}

NAN_METHOD(Iterator::New) {

  Database* database = Nan::ObjectWrap::Unwrap<Database>(info[0]->ToObject());

  //TODO: remove this, it's only here to make NL_STRING_OR_BUFFER_TO_MDVAL happy
  v8::Handle<v8::Function> callback;

  std::string* start = NULL;
  std::string* end = NULL;
  int limit = -1;

  v8::Local<v8::Value> id = info[1];

  v8::Local<v8::Object> optionsObj;

  v8::Local<v8::Object> ltHandle;
  v8::Local<v8::Object> lteHandle;
  v8::Local<v8::Object> gtHandle;
  v8::Local<v8::Object> gteHandle;

  bool startIsExclusive = false;
  bool endIsExclusive = false;
  
  if (info.Length() > 1 && info[2]->IsObject()) {
    optionsObj = v8::Local<v8::Object>::Cast(info[2]);

    if (optionsObj->Has(Nan::New("gte").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("gte").ToLocalChecked()))
          || optionsObj->Get(Nan::New("gte").ToLocalChecked())->IsString())) {

      v8::Local<v8::Value> startBuffer = optionsObj->Get(Nan::New("gte").ToLocalChecked()).As<v8::Object>();

      // ignore start if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(startBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_start, startBuffer, start)
        start = new std::string((const char*)_start.mv_data, _start.mv_size);
      }
    }
 
    if (optionsObj->Has(Nan::New("gt").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("gt").ToLocalChecked()))
          || optionsObj->Get(Nan::New("gt").ToLocalChecked())->IsString())) {

      if (start != NULL) {
        return Nan::ThrowError("Only one of 'gt' or 'gte' is allowed");
      }
 
      v8::Local<v8::Value> startBuffer = optionsObj->Get(Nan::New("gt").ToLocalChecked()).As<v8::Object>();

      // ignore start if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(startBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_start, startBuffer, start)
        start = new std::string((const char*)_start.mv_data, _start.mv_size);
        startIsExclusive = true;
      }
    }
    
    if (optionsObj->Has(Nan::New("lte").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("lte").ToLocalChecked()))
          || optionsObj->Get(Nan::New("lte").ToLocalChecked())->IsString())) {

      v8::Local<v8::Value> endBuffer = optionsObj->Get(Nan::New("lte").ToLocalChecked()).As<v8::Object>();

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(endBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_end, endBuffer, end)
        end = new std::string((const char*)_end.mv_data, _end.mv_size);
      }
    }
    
    if (optionsObj->Has(Nan::New("lt").ToLocalChecked())
        && (node::Buffer::HasInstance(optionsObj->Get(Nan::New("lt").ToLocalChecked()))
          || optionsObj->Get(Nan::New("lt").ToLocalChecked())->IsString())) {

      if (end != NULL) {
        return Nan::ThrowError("Only one of 'lt' or 'lte' is allowed");
      }
 
      v8::Local<v8::Value> endBuffer = optionsObj->Get(Nan::New("lt").ToLocalChecked()).As<v8::Object>();

      // ignore end if it has size 0 since a Slice can't have length 0
      if (StringOrBufferLength(endBuffer) > 0) {
        NL_STRING_OR_BUFFER_TO_MDVAL(_end, endBuffer, end)
        end = new std::string((const char*)_end.mv_data, _end.mv_size);
        endIsExclusive = true;
      }
    }
 
    if (!optionsObj.IsEmpty() && optionsObj->Has(Nan::New("limit").ToLocalChecked())) {
      limit =
        v8::Local<v8::Integer>::Cast(optionsObj->Get(Nan::New("limit").ToLocalChecked()))->Value();
    }
  }

  bool reverse = BooleanOptionValue(optionsObj, "reverse", false );
  bool keys = BooleanOptionValue(optionsObj, "keys", true);
  bool values = BooleanOptionValue(optionsObj, "values", true);
  bool keyAsBuffer = BooleanOptionValue(
      optionsObj
    , "keyAsBuffer"
    , true
  );
  bool valueAsBuffer = BooleanOptionValue(
      optionsObj
    , "valueAsBuffer"
    , false
  );
  
  if (reverse) {
    std::string *tmpKey = start;
    start = end;
    end = tmpKey;
    bool tmpExclusive = startIsExclusive;
    startIsExclusive = endIsExclusive;
    endIsExclusive = tmpExclusive;
  }

  Iterator* iterator = new Iterator(
      database
    , (uint32_t)id->Int32Value()
    , start
    , end
    , reverse
    , keys
    , values
    , limit
    , keyAsBuffer
    , valueAsBuffer
    , startIsExclusive
    , endIsExclusive
  );
  iterator->Wrap(info.This());

  //std::cerr << "New Iterator " << iterator->id << std::endl;

  info.GetReturnValue().Set(info.This());
}

} // namespace nlmdb
