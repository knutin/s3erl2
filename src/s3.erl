%% @doc Library for accessing Amazons S3 database.
-module(s3).

-export([get/2, get/3,
         put/4, put/5, put/6,
         delete/2, delete/3,
         stats/0]).

get(Bucket, Key) ->
    get(Bucket, Key, 5000).
get(Bucket, Key, Timeout) ->
    call({request, {get, Bucket, Key}}, Timeout).

put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, 5000).
put(Bucket, Key, Value, ContentType, Timeout) ->
    put(Bucket, Key, Value, ContentType, Timeout, []).
put(Bucket, Key, Value, ContentType, Timeout, Headers) ->
    call({request, {put, Bucket, Key, Value, ContentType, Headers}}, Timeout).

delete(Bucket, Key) ->
    delete(Bucket, Key, 5000).

delete(Bucket, Key, Timeout) ->
    call({request, {delete, Bucket, Key}}, Timeout).

stats() -> call(get_stats, 5000).

call(Request, Timeout) ->
    gen_server:call(s3_server, Request, Timeout).
