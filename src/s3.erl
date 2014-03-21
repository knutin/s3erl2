%% @doc Library for accessing Amazons S3 database.
-module(s3).

-export([get/2, get/3]).
-export([put/4, put/5, put/6]).
-export([delete/2, delete/3]).
-export([list/4, list/5]).
-export([fold/4]).
-export([stats/0]).
-export([signed_url/3, signed_url/4]).

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
    get(Bucket, Key, 5000, []).

-spec get(Bucket::bucket(), Key::key(), timeout() | [header()]) ->
                 {ok, [ResponseHeaders::header()], Body::value()} |
                 {ok, Body::value()} | term() |
                 {ok, not_modified}.
get(Bucket, Key, Timeout) when is_integer(Timeout) ->
    get(Bucket, Key, Timeout, []);
get(Bucket, Key, Headers) when is_list(Headers) ->
    get(Bucket, Key, 5000, Headers).

-spec get(Bucket::bucket(), Key::key(), [header()], timeout()) ->
                 {ok, [ResponseHeaders::header()], Body::value()} |
                 {ok, Body::value()} | term() |
                 {ok, not_modified}.
get(Bucket, Key, Timeout, Headers) ->
    call({request, {get, Bucket, Key, Headers}}, Timeout).


put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, 5000).

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

list(Bucket, Prefix, MaxKeys, Marker) ->
    list(Bucket, Prefix, MaxKeys, Marker, 5000).

list(Bucket, Prefix, MaxKeys, Marker, Timeout) ->
    call({request, {list, Bucket, Prefix, integer_to_list(MaxKeys), Marker}},
         Timeout).

-spec fold(Bucket::string(), Prefix::string(),
           FoldFun::fun((Key::string(), Acc::term()) -> NewAcc::term()),
           InitAcc::term()) -> FinalAcc::term().
fold(Bucket, Prefix, F, Acc) ->
    case s3:list(Bucket, Prefix, 100, "") of
        {ok, Keys} when is_list(Keys) ->
            do_fold(Bucket, Prefix, F, Keys, Acc);
        %% we only expect an error on the first call to list.
        {ok, not_found} -> {error, not_found};
        {error, Rsn} -> {error, Rsn}
    end.

do_fold(Bucket, Prefix, F, [Last], Acc) ->
    NewAcc = F(Last, Acc),
    %% get next part of the keys from the backup
    {ok, Keys} = list(Bucket, Prefix, 100, Last),
    do_fold(Bucket, Prefix, F, Keys, NewAcc);
do_fold(Bucket, Prefix, F, [H|T], Acc) ->
    %% this is the normal (recursive) case.
    do_fold(Bucket, Prefix, F, T, F(H, Acc));
do_fold(_Bucket, _Prefix, _F, [], Acc) -> Acc. %% done

stats() -> call(get_stats, 5000).

signed_url(Bucket, Key, Expires) ->
    call({request, {signed_url, Bucket, Key, Expires}}, 5000).

signed_url(Bucket, Key, Expires, Timeout) ->
    call({request, {signed_url, Bucket, Key, Expires}}, Timeout).

call(Request, Timeout) ->
    gen_server:call(s3_server, Request, Timeout).
