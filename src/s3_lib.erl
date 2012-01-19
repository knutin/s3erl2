%%
%% Blocking stateless library functions for working with Amazon S3.
%%
-module(s3_lib).

%% API
-export([get/3, put/5, delete/3]).

-include_lib("xmerl/include/xmerl.hrl").
-include("s3.hrl").

%%
%% API
%%

-spec get(#config{}, bucket(), key()) -> {ok, body()} | {error, any()}.
get(Config, Bucket, Key) ->
    do_get(Config, Bucket, Key).

-spec put(#config{}, bucket(), key(), body(), contenttype()) ->
                 {ok, etag()} | {error, any()}.
put(Config, Bucket, Key, Value, ContentType) ->
    do_put(Config, Bucket, Key, Value, ContentType).

delete(Config, Bucket, Key) ->
    do_delete(Config, Bucket, Key).

%%
%% INTERNAL HELPERS
%%

do_put(Config, Bucket, Key, Value, ContentType) ->
    case request(Config, put, Bucket, Key, [], Value, ContentType) of
        {ok, Headers, _Body} ->
            {"Etag", Etag} = lists:keyfind("Etag", 1, Headers),
            {ok, Etag};
        {error, _} = Error ->
            Error
    end.

do_get(Config, Bucket, Key) ->
    case request(Config, get, Bucket, Key, [], <<>>, "") of
        {ok, _Headers, Body} ->
            {ok, Body};
        {ok, not_found} ->
            {ok, not_found};
        Error ->
            Error
    end.

do_delete(Config, Bucket, Key) ->
    request(Config, delete, Bucket, Key, [], <<>>, "").




%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------


build_host(Bucket) ->
    [Bucket, ".s3.amazonaws.com"].

build_url(undefined, Bucket, Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]);
build_url(Endpoint, _Bucket, Path) ->
    lists:flatten(["http://", Endpoint, "/", Path]).

request(Config, Method, Bucket, Path, UserHeaders, Body, ContentType) ->
    Date = httpd_util:rfc1123_date(),
    Url = build_url(Config#config.endpoint, Bucket, Path),

    Headers = [{"Content-Type", ContentType} | UserHeaders],

    Signature = sign(Config#config.secret_access_key,
                     stringToSign(Method, "", ContentType,
                                  Date, Bucket, Path, Headers)),

    Auth = ["AWS ", Config#config.access_key, ":", Signature],
    FullHeaders = [{"Authorization", Auth},
                   {"Host", build_host(Bucket)},
                   {"Date", Date},
                   {"Connection", "keep-alive"}
                   | Headers],

    case lhttpc:request(Url, Method, FullHeaders,
                        Body, Config#config.timeout) of
        {ok, {{200, _}, ResponseHeaders, ResponseBody}} ->
            {ok, ResponseHeaders, ResponseBody};
        {ok, {{404, "Not Found"}, _, _}} ->
            {ok, not_found};
        {ok, {{204, "No Content"}, _, _}} ->
            {ok, not_found};
        {ok, {_Code, _ResponseHeaders, ResponseBody} = Res} ->
            error_logger:info_msg("Res: ~p~n", [Res]),
            {error, parseErrorXml(ResponseBody)};
        {error, Reason} ->
            {error, Reason}
    end.


parseErrorXml(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Xml)),
    [#xmlText{value=ErrorCode}] = xmerl_xpath:string("/Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}] = xmerl_xpath:string("/Error/Message/text()",
                                                        XmlDoc),
    {ErrorCode, ErrorMessage}.


%%
%% Signing
%%

is_amz_header(<<"x-amz-", _/binary>>) -> true;
is_amz_header(_)                      -> false.

canonicalizedAmzHeaders(AllHeaders) ->
    AmzHeaders = [{string:to_lower(K),V} || {K,V} <- AllHeaders, is_amz_header(K)],
    Strings = lists:map(
                fun s3util:join/1,
                s3util:collapse(
                  lists:keysort(1, AmzHeaders) ) ),
    s3util:string_join(lists:map( fun (S) -> S ++ "\n" end, Strings), "").

canonicalizedResource("", "") -> "/";
canonicalizedResource(Bucket, "") -> "/" ++ Bucket ++ "/";
canonicalizedResource(Bucket, Path) -> "/" ++ Bucket ++ "/" ++ Path.

stringToSign(Verb, ContentMD5, ContentType, Date, Bucket, Path, OriginalHeaders) ->
    VerbString = string:to_upper(atom_to_list(Verb)),
    Parts = [VerbString, ContentMD5, ContentType, Date,
             canonicalizedAmzHeaders(OriginalHeaders)],
    s3util:string_join(Parts, "\n") ++ canonicalizedResource(Bucket, Path).

sign(Key,Data) ->
    base64:encode(crypto:sha_mac(Key,Data)).

