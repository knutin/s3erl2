%%
%% Blocking stateless library functions for working with Amazon S3.
%%
-module(s3_lib).

%% API
-export([get/5, put/7, delete/4, list/6, signed_url/6]).

-include_lib("xmerl/include/xmerl.hrl").
-include("../include/s3.hrl").

-define(b2l(B), binary_to_list(B)).

%%
%% API
%%

-spec get(#config{}, profile(), bucket(), key(), [header()]) ->
                 {ok, {headers(), value()}} | {error, error_reason()}.
get(Config, Profile, Bucket, Key, Headers) ->
    do_get(Config, Profile, Bucket, ?b2l(Key), Headers).

-spec put(#config{}, profile(), bucket(), key(), value(), contenttype(), headers()) ->
                 {ok, etag()} | {error, error_reason()}.
put(Config, Profile, Bucket, Key, Value, ContentType, Headers) ->
    NewHeaders = [{<<"Content-Type">>, ContentType} | Headers],
    do_put(Config, Profile, Bucket, ?b2l(Key), Value, NewHeaders).

delete(Config, Profile, Bucket, Key) ->
    do_delete(Config, Profile, Bucket, ?b2l(Key)).

list(Config, Profile, Bucket, Prefix, MaxKeys, Marker) ->
    Key = ["?", "prefix=", Prefix, "&", "max-keys=", MaxKeys, "&marker=", Marker],
    case request(Config, Profile, get, Bucket, lists:flatten(Key), [], <<>>) of
        {ok, _Headers, Body} ->
            {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
            Keys = lists:map(fun (#xmlText{value = K}) -> list_to_binary(K) end,
                             xmerl_xpath:string(
                               "/ListBucketResult/Contents/Key/text()", XmlDoc)),

            {ok, Keys};
        {ok, not_found} ->
            {ok, not_found};
        {error, _} = Error ->
            Error
    end.

signed_url(_Config, Profile, Bucket, Key, Method, Expires) ->
    Signature = sign(secret_access_key(Profile),
                     stringToSign(Method, "", integer_to_list(Expires),
                                  Bucket, Key, "")),
    Url = build_full_url(endpoint(Profile), Bucket, Key),
    SignedUrl = [Url, "?", "AWSAccessKeyId=", access_key(Profile), "&",
                 "Expires=", integer_to_list(Expires), "&", "Signature=",
                 http_uri:encode(binary_to_list(Signature))],
    lists:flatten(SignedUrl).


%%
%% INTERNAL HELPERS
%%

do_put(Config, Profile, Bucket, Key, Value, Headers) ->
    case request(Config, Profile, put, Bucket, Key, Headers, Value) of
        {ok, RespHeaders, Body} ->
            case lists:keyfind("Etag", 1, RespHeaders) of
                {"Etag", Etag} ->
                    %% for objects
                    {ok, Etag};
                false when Key == "" andalso Value == "" ->
                    %% for bucket
                    ok;
                false when Value == "" orelse Value == <<>> ->
                    %% for bucket-to-bucket copy
                    {ok, parseCopyXml(Body)}
            end;
        {ok, not_found} -> %% eg. bucket doesn't exist.
            {ok, not_found};
        {error, _} = Error ->
            Error
    end.

do_get(Config, Profile, Bucket, Key, Headers) ->
    case request(Config, Profile, get, Bucket, Key, Headers, <<>>) of
        {ok, ResponseHeaders, Body} ->
            if Config#config.return_headers ->
                    {ok, ResponseHeaders, Body};
               true ->
                    {ok, Body}
            end;
        {ok, not_found} ->
            {ok, not_found};
        Error ->
            Error
    end.

do_delete(Config, Profile, Bucket, Key) ->
    request(Config, Profile, delete, Bucket, Key, [], <<>>).


%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

build_host(Bucket) ->
    [Bucket, ".s3.amazonaws.com"].

build_host(Endpoint, Bucket) ->
    [Endpoint, "/", Bucket].

build_url(undefined, Bucket, Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]);
build_url(Endpoint, _Bucket, Path) ->
    lists:flatten(["http://", Endpoint, "/", Path]).

build_full_url(undefined, Bucket, Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]);
build_full_url(Endpoint, Bucket, Path) ->
    lists:flatten(["http://", build_host(Endpoint, Bucket), "/", Path]).

request(Config, Profile, Method, Bucket, Path, Headers, Body) ->
    Date = httpd_util:rfc1123_date(),
    Url = build_url(endpoint(Profile), Bucket, Path),
    StringHeaders = [{?b2l(K), ?b2l(V)} || {K, V} <- Headers],
    Signature = sign(secret_access_key(Profile),
                     stringToSign(Method, "", Date, ?b2l(Bucket), Path, StringHeaders)),

    Auth = ["AWS ", access_key(Profile), ":", Signature],
    FullHeaders = [{"Authorization", Auth},
                   {"Host", build_host(Bucket)},
                   {"Date", Date},
                   {"Connection", "keep-alive"}
                   | StringHeaders],
    Options = [{max_connections, Config#config.max_concurrency}],

    do_request(Url, Method, FullHeaders, Body, Config#config.timeout, Options).

do_request(Url, Method, Headers, Body, Timeout, Options) ->
    case lhttpc:request(Url, Method, Headers, Body, Timeout, Options) of
        {ok, {{200, _}, ResponseHeaders, ResponseBody}} ->
            {ok, ResponseHeaders, ResponseBody};
        {ok, {{204, "No Content" ++ _}, ResHead, ResBody}} ->
            %%error_logger:info_msg("204~nheaders: ~p~nbody: ~p~n", [ResHead, ResBody]),
            {error, document_not_found};
        {ok, {{307, "Temporary Redirect" ++ _}, ResponseHeaders, _ResponseBody}} ->
            {"Location", Location} = lists:keyfind("Location", 1, ResponseHeaders),
            do_request(Location, Method, Headers, Body, Timeout, Options);
        {ok, {{404, "Not Found" ++ _}, ResHead, ResBody}} ->
            %%error_logger:info_msg("404~nheaders: ~p~nbody: ~p~n", [ResHead, ResBody]),
            {error, document_not_found};
        {ok, {Code, _ResponseHeaders, <<>>}} ->
            {error, Code};
        {ok, {_Code, _ResponseHeaders, ResponseBody}} ->
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


parseCopyXml(Xml) ->
    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Xml)),
    %% xmerl doesn't parse &quot; escape character very well
    case xmerl_xpath:string("/CopyObjectResult/ETag/text()", XmlDoc) of
        [#xmlText{value=Etag}, #xmlText{value="\""}] -> Etag ++ "\"";
        [#xmlText{value=Etag}] -> Etag
    end.


access_key({AccessKey, _, _})              -> ?b2l(AccessKey).
secret_access_key({_, SecretAccessKey, _}) -> ?b2l(SecretAccessKey).
endpoint({_, _, Endpoint})                 -> ?b2l(Endpoint).


%%
%% Signing
%%

is_amz_header(<<"x-amz-", _/binary>>) -> true; %% this is not working.
is_amz_header("x-amz-"++ _)           -> true;
is_amz_header(_)                      -> false.

canonicalizedAmzHeaders("") -> "";
canonicalizedAmzHeaders(AllHeaders) ->
    AmzHeaders = [{string:to_lower(K),V} || {K,V} <- AllHeaders, is_amz_header(K)],
    Strings = lists:map(
                fun s3util:join/1,
                s3util:collapse(
                  lists:keysort(1, AmzHeaders) ) ),
    s3util:string_join(lists:map( fun (S) -> S ++ "\n" end, Strings), "").

canonicalizedResource("", "")       -> "/";
canonicalizedResource(Bucket, "")   -> ["/", Bucket, "/"];
canonicalizedResource(Bucket, Path) when is_list(Path) ->
    canonicalizedResource(Bucket, list_to_binary(Path));
canonicalizedResource(Bucket, Path) ->
    case binary:split(Path, <<"?">>) of
        [URL, _SubResource] ->
            %% TODO: Possible include the sub resource if it should be
            %% included
            ["/", Bucket, "/", URL];
        [URL] ->
            ["/", Bucket, "/", URL]
    end.

stringToSign(Verb, ContentMD5 = "", Date, Bucket = "", Path,
             OriginalHeaders = "") ->
    VerbString = string:to_upper(atom_to_list(Verb)),
    Parts = [VerbString, ContentMD5, "", Date,
             canonicalizedAmzHeaders(OriginalHeaders)],
    [s3util:string_join(Parts, "\n"), canonicalizedResource(Bucket, Path)];
stringToSign(Verb, ContentMD5, Date, Bucket, Path, OriginalHeaders) ->
    VerbString = string:to_upper(atom_to_list(Verb)),
    ContentType = proplists:get_value("Content-Type", OriginalHeaders, ""),
    Parts = [VerbString, ContentMD5, ContentType, Date,
             canonicalizedAmzHeaders(OriginalHeaders)],
    [s3util:string_join(Parts, "\n"), canonicalizedResource(Bucket, Path)].

sign(Key,Data) ->
    base64:encode(crypto:hmac(sha, Key, lists:flatten(Data))).
