%% @doc Library for accessing Amazons S3 database.
-module(s3).
-include("s3.hrl").

-export([get/2, get/5]).
-export([put/4, put/7]).
-export([delete/2, delete/4]).
-export([list/4, list/6]).
-export([fold/4, fold/6]).
-export([stats/0]).
-export([signed_url/3, signed_url/6]).

-export_type([key/0, value/0, bucket/0]).

%%
%% API
%%

get(Bucket, Key) ->
    get(Bucket, Key, [], default, 5000).

-spec get(bucket(), key(), [header()], profile(), timeout()) ->
                 {ok, {headers(), value()}} |
                 {error, error_reason()}.
get(Bucket, Key, Headers, Profile, Timeout) ->
    call({request, {get, Bucket, Key, Headers}, Profile}, Timeout).



put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, [], default, 5000).

-spec put(bucket(), key(), value(), contenttype(), headers(), profile(), timeout()) ->
                 {ok, etag()} | {error, error_reason()}.
put(Bucket, Key, Value, ContentType, Headers, Profile, Timeout) ->
    call({request, {put, Bucket, Key, Value, ContentType, Headers}, Profile}, Timeout).


delete_bucket(Bucket) ->
    delete_bucket(Bucket, default, 5000).

delete_bucket(Bucket, Profile, Timeout) ->
    call({request, {delete_bucket, Bucket}, Profile}, Timeout).


delete(Bucket, Key) ->
    delete(Bucket, Key, default, 5000).

delete(Bucket, Key, Profile, Timeout) ->
    call({request, {delete, Bucket, Key}, Profile}, Timeout).


list(Bucket, Prefix, MaxKeys, Marker) ->
    list(Bucket, Prefix, MaxKeys, Marker, default, 5000).

list(Bucket, Prefix, MaxKeys, Marker, Profile, Timeout) ->
    call({request, {list, Bucket, Prefix, integer_to_list(MaxKeys), Marker}, Profile},
         Timeout).


fold(Bucket, Prefix, F, Acc) ->
    fold(Bucket, Prefix, F, Acc, default, 5000).

-spec fold(bucket(), binary(),
           FoldFun::fun((key(), Acc::term()) -> NewAcc::term()),
           InitAcc::term(), profile(), timeout()) -> FinalAcc::term().
fold(Bucket, Prefix, F, Acc, Profile, Timeout) ->
    case s3:list(Bucket, Prefix, 100, "", Profile, Timeout) of
        {ok, Keys} when is_list(Keys) ->
            do_fold(Bucket, Prefix, F, Keys, Acc, Profile, Timeout);
        %% we only expect an error on the first call to list.
        {ok, not_found} -> {error, not_found};
        {error, Rsn} -> {error, Rsn}
    end.

do_fold(Bucket, Prefix, F, [Last], Acc, Profile, Timeout) ->
    NewAcc = F(Last, Acc),
    %% get next part of the keys from the backup
    {ok, Keys} = list(Bucket, Prefix, 100, Last, Profile, Timeout),
    do_fold(Bucket, Prefix, F, Keys, NewAcc, Profile, Timeout);
do_fold(Bucket, Prefix, F, [H|T], Acc, Profile, Timeout) ->
    %% this is the normal (recursive) case.
    do_fold(Bucket, Prefix, F, T, F(H, Acc), Profile, Timeout);
do_fold(_Bucket, _Prefix, _F, [], Acc, _Profile, _Timeout) ->
    Acc. %% done

stats() -> call(get_stats, 5000).

signed_url(Bucket, Key, Expires) ->
    signed_url(Bucket, Key, Expires, get, default, 5000).

-spec signed_url(bucket(), key(), pos_integer(), atom(), profile(), timeout()) -> list().
signed_url(Bucket, Key, Expires, Method, Profile, Timeout) ->
    call({request, {signed_url, Bucket, Key, Method, Expires}, Profile}, Timeout).

call(Request, Timeout) ->
    try
        gen_server:call(s3_server, Request, Timeout)
    catch
        exit:{timeout, {gen_server, call, _}} ->
            {error, timeout}
    end.
