%% @doc Library for accessing Amazons S3 database.
-module(s3).

-export([get/2, get/3,
         put/4, put/5,
         delete/2, delete/3]).


get(Bucket, Key) ->
    get(Bucket, Key, 5000).
get(Bucket, Key, Timeout) ->
    call({get, Bucket, Key}, Timeout).

put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, 5000).
put(Bucket, Key, Value, ContentType, Timeout) ->
    call({put, Bucket, Key, Value, ContentType}, Timeout).

delete(Bucket, Key) ->
    delete(Bucket, Key, 5000).

delete(Bucket, Key, Timeout) ->
    call({delete, Bucket, Key}, Timeout).


call(Request, Timeout) ->
    gen_server:call(s3_server, Request, Timeout).
