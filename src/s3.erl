%% @doc Library for accessing Amazons S3 database.
-module(s3).

-export([get/2, get/3,
         put/4, put/5, put/6,
         delete/2, delete/3,
         stats/0]).

-type value() :: string() | binary().
-export_type([value/0]).

-type header() :: {string(), string()}.

-type bucket() :: string().
-export_type([bucket/0]).

-type key() :: string().
-export_type([key/0]).

-spec get(Bucket::bucket(), Key::key()) ->
                 {ok, [ResponseHeaders::header()], Body::value()} |
                 {ok, Body::value()} | term().
get(Bucket, Key) ->
    get(Bucket, Key, 5000).

-spec get(Bucket::bucket(), Key::key(), timeout()) ->
                 {ok, [ResponseHeaders::header()], Body::value()} |
                 {ok, Body::value()} | term().
get(Bucket, Key, Timeout) ->
    call({request, {get, Bucket, Key}}, Timeout).

-spec put(Bucket::bucket(), Key::key(), Value::value(),
          ContentType::string()) ->
                 {ok, Etag::any()} | ok | any().
put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, 5000).
-spec put(Bucket::bucket(), Key::key(), Value::value(),
          ContentType::string(), timeout()) ->
                 {ok, Etag::any()} | ok | any().
put(Bucket, Key, Value, ContentType, Timeout) ->
    put(Bucket, Key, Value, ContentType, Timeout, []).
-spec put(Bucket::bucket(), Key::key(), Value::value(),
          ContentType::string(), timeout(), list(header())) ->
                 {ok, Etag::any()} | ok | any().
put(Bucket, Key, Value, ContentType, Timeout, Headers) ->
    call({request, {put, Bucket, Key, Value, ContentType, Headers}}, Timeout).

delete(Bucket, Key) ->
    delete(Bucket, Key, 5000).

delete(Bucket, Key, Timeout) ->
    call({request, {delete, Bucket, Key}}, Timeout).

stats() -> call(get_stats, 5000).

call(Request, Timeout) ->
    gen_server:call(s3_server, Request, Timeout).
