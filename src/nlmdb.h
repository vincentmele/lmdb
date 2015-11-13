/* Copyright (c) 2013 Rod Vagg
 * MIT +no-false-attribs License <https://github.com/rvagg/lmdb/blob/master/LICENSE>
 */

#ifndef NL_LMDB_H
#define NL_LMDB_H

#include <node.h>
#include <node_buffer.h>
#include <string>
#include <lmdb.h>
#include <nan.h>

typedef struct md_status {
  int code;
  std::string error;
} md_status;

static inline char* FromV8String(v8::Local<v8::Value> from) {
  size_t sz_;
  char* to;
  v8::Local<v8::String> toStr = from->ToString();
  sz_ = toStr->Utf8Length();
  to = new char[sz_ + 1];
  toStr->WriteUtf8(to, -1, NULL, v8::String::NO_OPTIONS);
  return to;
}

static inline size_t StringOrBufferLength(v8::Local<v8::Value> obj) {
  return node::Buffer::HasInstance(obj->ToObject())
    ? node::Buffer::Length(obj->ToObject())
    : obj->ToString()->Utf8Length();
}

static inline bool BooleanOptionValue(
	v8::Local<v8::Object> options,
        const char* _key,
        bool def = false) {

  v8::Handle<v8::String> key = Nan::New(_key).ToLocalChecked();
  return !options.IsEmpty()
    && options->Has(key)
    ? options->Get(key)->BooleanValue()
    : def;
}

static inline bool BooleanOptionValueDefTrue(
      v8::Local<v8::Object> optionsObj
    , v8::Handle<v8::String> opt) {

  return optionsObj.IsEmpty()
    || !optionsObj->Has(opt)
    || optionsObj->Get(opt)->BooleanValue();
}

static inline uint32_t UInt32OptionValue(v8::Local<v8::Object> options,
                                      const char* _key,
                                      uint32_t def) {
  v8::Handle<v8::String> key = Nan::New(_key).ToLocalChecked();
  return !options.IsEmpty()
    && options->Has(key)
    && options->Get(key)->IsNumber()
    ? options->Get(key)->Uint32Value()
    : def;
}



static inline uint64_t UInt64OptionValue(
      v8::Local<v8::Object> optionsObj
    , v8::Handle<v8::String> opt
    , uint64_t def) {

  return !optionsObj.IsEmpty()
    && optionsObj->Has(opt)
    && optionsObj->Get(opt)->IsNumber()
      ? optionsObj->Get(opt)->IntegerValue()
      : def;
}

#define NL_CB_ERR_IF_NULL_OR_UNDEFINED(thing, name)                            \
  if (thing->IsNull() || thing->IsUndefined()) {                               \
    NL_RETURN_CALLBACK_OR_ERROR(callback, #name " cannot be `null` or `undefined`") \
  }

#define NL_RETURN_CALLBACK_OR_ERROR(callback, msg)                             \
  if (!callback.IsEmpty() && callback->IsFunction()) {                         \
    v8::Local<v8::Value> argv[] = {                                            \
      Nan::Error(msg)                                                            \
    };                                                                         \
    NL_RUN_CALLBACK(callback, argv, 1)                                         \
    return;                                                      \
  }                                                                            \
  return Nan::ThrowError(msg);

#define NL_RUN_CALLBACK(callback, argv, length)                                \
  v8::TryCatch try_catch;                                                      \
  Nan::MakeCallback(                                                             \
    Nan::GetCurrentContext()->Global(), callback, length, argv);                 \
  if (try_catch.HasCaught()) {                                                 \
    node::FatalException(try_catch);                                           \
  }


/* NL_METHOD_SETUP_COMMON setup the following objects:
 *  - Database* database
 *  - v8::Local<v8::Object> optionsObj (may be empty)
 *  - Nan::Persistent<v8::Function> callback (won't be empty)
 * Will NL_THROW_RETURN if there isn't a callback in arg 0 or 1
 */
#define NL_METHOD_SETUP_COMMON(name, optionPos, callbackPos)                   \
  if (info.Length() == 0) {                                                    \
    return Nan::ThrowError(#name "() requires a callback argument");             \
  }                                                                            \
  nlmdb::Database* database =                                                  \
    Nan::ObjectWrap::Unwrap<nlmdb::Database>(info.This());                    \
  v8::Local<v8::Object> optionsObj;                                            \
  v8::Local<v8::Function> callback;                                            \
  if (optionPos == -1 && info[callbackPos]->IsFunction()) {                    \
    callback = v8::Local<v8::Function>::Cast(info[callbackPos]);               \
  } else if (optionPos != -1 && info[callbackPos - 1]->IsFunction()) {         \
    callback = v8::Local<v8::Function>::Cast(info[callbackPos - 1]);           \
  } else if (optionPos != -1                                                   \
        && info[optionPos]->IsObject()                                         \
        && info[callbackPos]->IsFunction()) {                                  \
    optionsObj = v8::Local<v8::Object>::Cast(info[optionPos]);                 \
    callback = v8::Local<v8::Function>::Cast(info[callbackPos]);               \
  } else {                                                                     \
    return Nan::ThrowError(#name "() requires a callback argument");             \
  }

#define NL_METHOD_SETUP_COMMON_ONEARG(name) NL_METHOD_SETUP_COMMON(name, -1, 0)

// NOTE: this MUST be called on objects created by
// NL_STRING_OR_BUFFER_TO_MDVAL
static inline void DisposeStringOrBufferFromMDVal(
      v8::Local<v8::Value> handle
    , MDB_val val) {

  Nan::HandleScope scope;
  if (!node::Buffer::HasInstance(handle))
    delete[] (char*)val.mv_data;
}

// NOTE: must call DisposeStringOrBufferFromMDVal() on objects created here
#define NL_STRING_OR_BUFFER_TO_MDVAL(to, from, name)                           \
  size_t to ## Sz_;                                                            \
  char* to ## Ch_;                                                             \
  if (node::Buffer::HasInstance(from->ToObject())) {                           \
    to ## Sz_ = node::Buffer::Length(from->ToObject());                        \
    if (to ## Sz_ == 0) {                                                      \
      NL_RETURN_CALLBACK_OR_ERROR(callback, #name " cannot be an empty Buffer") \
    }                                                                          \
    to ## Ch_ = node::Buffer::Data(from->ToObject());                          \
  } else {                                                                     \
    v8::Local<v8::String> to ## Str = from->ToString();                        \
    to ## Sz_ = to ## Str->Utf8Length();                                       \
    if (to ## Sz_ == 0) {                                                      \
      NL_RETURN_CALLBACK_OR_ERROR(callback, #name " cannot be an empty String") \
    }                                                                          \
    to ## Ch_ = new char[to ## Sz_];                                           \
    to ## Str->WriteUtf8(                                                      \
        to ## Ch_                                                              \
      , -1                                                                     \
      , NULL                                                                   \
      , v8::String::NO_NULL_TERMINATION                                        \
    );                                                                         \
  }                                                                            \
  MDB_val to;                                                                  \
  to.mv_data = to ## Ch_;                                                      \
  to.mv_size = to ## Sz_;

//////////////
//////////////

// RWI: NEW SYNC FUNCTIONALITY

//////////////
//////////////

/* NL_METHOD_SETUP_SYNC setup the following objects:
 *  - Database* database
 *  - v8::Local<v8::Object> optionsObj (may be empty)
 *  - Nan::Persistent<v8::Function> callback (won't be empty)
 * Will NL_THROW_RETURN if there isn't a callback in arg 0 or 1
 */
/*#define NL_METHOD_SETUP_SYNC(name, optionPos)                                   \
  nlmdb::Database* database =                                                   \
    Nan::ObjectWrap::Unwrap<nlmdb::Database>(info.This());                     \
  v8::Local<v8::Object> optionsObj;                                             \
  if (optionPos != -1 && info[optionPos]->IsObject()) {                       \
    optionsObj = v8::Local<v8::Object>::Cast(info[optionPos]);                  \
  }
*/  
// NOTE: must call DisposeStringOrBufferFromMDVal() on objects created here
#define NL_STRING_OR_BUFFER_TO_MDVAL_SYNC(to, from, name)                      \
  size_t to ## Sz_;                                                            \
  char* to ## Ch_;                                                             \
  if (node::Buffer::HasInstance(from->ToObject())) {                           \
    to ## Sz_ = node::Buffer::Length(from->ToObject());                        \
    to ## Ch_ = node::Buffer::Data(from->ToObject());                          \
  } else {                                                                     \
    v8::Local<v8::String> to ## Str = from->ToString();                        \
    to ## Sz_ = to ## Str->Utf8Length();                                       \
    to ## Ch_ = new char[to ## Sz_];                                           \
    to ## Str->WriteUtf8(                                                      \
        to ## Ch_                                                              \
      , -1                                                                     \
      , NULL                                                                   \
      , v8::String::NO_NULL_TERMINATION                                        \
    );                                                                         \
  }                                                                            \
  MDB_val to;                                                                  \
  to.mv_data = to ## Ch_;                                                      \
  to.mv_size = to ## Sz_;


#endif

