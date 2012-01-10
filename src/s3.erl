%%
%% Blocking stateless library functions for working with Amazon S3.
%%
-module(s3).

%% API
-export([get/3, put/5]).

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
        {ok, _, Body} ->
            {ok, Body};
        {error, _} = Error ->
            Error
    end.



%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------



isAmzHeader(Header) ->
    case Header of
        <<"x-amz-", _/binary>> ->
            true;
        _ ->
            false
    end.

canonicalizedAmzHeaders(AllHeaders) ->
    AmzHeaders = [{string:to_lower(K),V} || {K,V} <- AllHeaders, isAmzHeader(K)],
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

build_host(Bucket) ->
    [Bucket, ".s3.amazonaws.com"].

build_url(Bucket,Path) ->
    lists:flatten(["http://", build_host(Bucket), "/", Path]).

request(Config, Method, Bucket, Path, UserHeaders, Body, ContentType) ->
    Date = httpd_util:rfc1123_date(),
    Url = build_url(Bucket, Path),

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

    ReqF = fun() ->
                   lhttpc:request(Url, Method, FullHeaders,
                                  Body, Config#config.timeout)
           end,
    case attempt(ReqF, Config) of
        {ok, {{200, _}, ResponseHeaders, ResponseBody}} ->
            {ok, ResponseHeaders, ResponseBody};
        {ok, {{404, "Not Found"}, _, _}} ->
            {error, not_found};
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


attempt(F, Config) ->
    attempt(F, 0, Config).

attempt(F, Attempts, #config{retry_callback = Callback} = Config) ->
    case catch(F()) of
        {error, _} = Error when Attempts =:= Config#config.max_retries ->
            Error;
        {'EXIT', Reason} when Attempts =:= Config#config.max_retries ->
            {error, Reason};

        {error, Reason} when (Reason =:= connect_timeout orelse
                              Reason =:= timeout) ->

            Callback(Reason, abs(Attempts - Config#config.max_retries) + 1),
            timer:sleep(Config#config.retry_delay),
            attempt(F, Attempts + 1, Config);
        {'EXIT', {econnrefused, _}} ->
            Callback(econnrefused, abs(Attempts - Config#config.max_retries) + 1),
            timer:sleep(Config#config.retry_delay),
            attempt(F, Attempts + 1, Config);
        {error, Reason} ->
            {error, Reason};
        R ->
            R
    end.
