-module(s3_test).
-include_lib("eunit/include/eunit.hrl").

integration_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [
      ?_test(get_put())
     ,?_test(concurrency_limit())
     ,?_test(timeout_retry())
     ,?_test(slow_endpoint())
     %% ,?_test(permission_denied()) % Fails with fakes3
     ,?_test(fold())
     ,?_test(list_objects())
     ,?_test(signed_url())
     ,?_test(reload_config())
     ,?_test(callback())
     ]}.

setup() ->
    {ok, _} = application:ensure_all_started(lhttpc),
    {ok, _} = s3_server:start_link([]),
    [].

teardown([]) ->
    s3_server:stop().



%%
%% TESTS
%%


get_put() ->
    s3:delete(bucket(), <<"foo">>, default_profile(), 5000),
    s3:delete(bucket(), <<"foo-copy">>, default_profile(), 5000),

    ?assertEqual({error, document_not_found},
                 s3:get(bucket(), <<"foo">>, [], default_profile(), 5000)),

    ?assertMatch({ok, _},
                 s3:put(bucket(), <<"foo">>, <<"bazbar">>, <<"text/plain">>,
                        [], default_profile(), 5000)),
    ?assertEqual({ok, <<"bazbar">>},
                 s3:get(bucket(), <<"foo">>, [], default_profile(), 5000)),

    ?assertMatch({ok, _},
                 s3:put(bucket(), <<"foo-copy">>, <<>>, <<"text/plain">>,
                        [{<<"x-amz-copy-source">>, <<(bucket())/binary, "/foo">>}],
                        default_profile(), 5000)),
    ?assertEqual({ok, <<"bazbar">>},
                 s3:get(bucket(), <<"foo-copy">>, [], default_profile(), 5000)).

permission_denied() ->
    ?assertEqual({error, {"AccessDenied", "Access Denied"}},
                 s3:put(<<"foobar">>, <<"foo">>, <<"bazbar">>, <<"text/plain">>,
                        [], default_profile(), 5000)).

concurrency_limit() ->
    Parent = self(),
    MaxConcurrencyCB = fun (N) -> Parent ! {max_concurrency, N} end,

    s3_server:reload_config([{max_concurrency_callback, MaxConcurrencyCB},
                             {max_concurrency, 3}]),

    meck:new(s3_lib),
    GetF = fun (_, _, _, _, _) -> timer:sleep(50), {ok, <<"bazbar">>} end,
    meck:expect(s3_lib, get, GetF),

    DoGet = fun () -> s3:get(bucket(), <<"foo">>, [], default_profile(), 5000) end,

    P1 = spawn(fun() -> Parent ! {self(), DoGet()} end),
    P2 = spawn(fun() -> Parent ! {self(), DoGet()} end),
    P3 = spawn(fun() -> Parent ! {self(), DoGet()} end),
    P4 = spawn(fun() -> Parent ! {self(), DoGet()} end),

    receive M0 -> ?assertEqual({max_concurrency, 3}, M0) end,
    ?assertEqual([{error, max_concurrency},
                  {ok, <<"bazbar">>},
                  {ok, <<"bazbar">>},
                  {ok, <<"bazbar">>}],
                 lists:sort([receive {P, M} -> M  end || P <- [P1,P2,P3,P4]])),

    ?assertEqual({ok, <<"bazbar">>}, s3:get(bucket(), <<"foo">>, [], default_profile(), 5000)),

    ?assert(meck:validate(s3_lib)),
    meck:unload(s3_lib).

timeout_retry() ->
    Parent = self(),
    RetryCb = fun (Reason, Attempt) ->
                      Parent ! {Reason, Attempt}
              end,

    s3_server:reload_config([{timeout, 10},
                             {retry_callback, RetryCb},
                             {retry_delay, 10}]),

    meck:new(lhttpc),
    TimeoutF = fun (_, _, _, _, _, _) ->
                       timer:sleep(50),
                       {error, timeout}
               end,
    meck:expect(lhttpc, request, TimeoutF),
    %% meck:expect(lhttpc, request, TimeoutF),
    %% meck:expect(lhttpc, request, TimeoutF),

    ?assertEqual({error, timeout},
                 s3:get(bucket(), <<"foo">>, [], default_profile(), 5000)),

    receive M1 -> ?assertEqual({timeout, 0}, M1) end,
    receive M2 -> ?assertEqual({timeout, 1}, M2) end,
    receive M3 -> ?assertEqual({timeout, 2}, M3) end,
    receive _  -> ?assert(false) after 1 -> ok end,

    ?assert(meck:validate(lhttpc)),
    meck:unload(lhttpc).

slow_endpoint() ->
    Port = webserver:start(gen_tcp, [fun very_slow_response/5]),
    PortBin = list_to_binary(integer_to_list(Port)),
    s3_server:reload_config([{timeout, 10},
                             {retry_delay, 10}]),
    Profile = {<<"foo">>, <<"bar">>, <<"localhost:", PortBin/binary>>},
    ?assertEqual({error, timeout}, s3:get(bucket(), <<"foo">>, [], Profile, 100)).

list_objects() ->
    {ok, _} = s3:put(bucket(), <<"1/1">>, <<"foo">>, <<"text/plain">>, [], default_profile(), 5000),
    {ok, _} = s3:put(bucket(), <<"1/2">>, <<"foo">>, <<"text/plain">>, [], default_profile(), 5000),
    {ok, _} = s3:put(bucket(), <<"1/3">>, <<"foo">>, <<"text/plain">>, [], default_profile(), 5000),
    {ok, _} = s3:put(bucket(), <<"2/1">>, <<"foo">>, <<"text/plain">>, [], default_profile(), 5000),


    ?assertEqual({ok, [<<"1/1">>, <<"1/2">>, <<"1/3">>]},
                 s3:list(bucket(), <<"1/">>, 10, <<"">>, default_profile(), 5000)),

    ?assertEqual({ok, [<<"1/3">>]},
                 s3:list(bucket(), <<"1/">>, 10, <<"1/2">>, default_profile(), 5000)),

    %% List all, includes keys from other tests.
    ?assertEqual({ok, [<<"1/1">>, <<"1/2">>, <<"1/3">>, <<"2/1">>,
                       <<"foo">>, <<"foo-copy">>]},
                 s3:list(bucket(), <<"">>, 10, <<"">>, default_profile(), 5000)).

fold() ->
    %% Depends on earlier tests to setup data.
    ?assertEqual([<<"1/3">>, <<"1/2">>, <<"1/1">>],
                 s3:fold(bucket(), <<"1/">>, fun(Key, Acc) -> [Key|Acc] end, [],
                         default_profile(), 5000)),

     %% List all, includes keys from other tests.
    ?assertEqual([<<"foo-copy">>, <<"foo">>, <<"2/1">>, <<"1/3">>, <<"1/2">>,
                  <<"1/1">>],
                 s3:fold(bucket(), <<"">>, fun(Key, Acc) -> [Key|Acc] end, [],
                        default_profile(), 5000)).

callback() ->
    Parent = self(),
    F = fun (Request, Response, ElapsedUs) ->
                Parent ! {Request, Response, ElapsedUs}
        end,

    ok = s3_server:reload_config([{post_request_callback, F}]),

    {ok, _} = s3:put(bucket(), <<"foo">>, <<"bar">>, <<"text/plain">>, [], default_profile(), 5000),
    {ok, _} = s3:get(bucket(), <<"foo">>, [], default_profile(), 5000),

    receive M1 ->
            fun () ->
                    {Request, Response, _ElapsedUs} = M1,
                    ?assertEqual({put, bucket(), <<"foo">>, <<"bar">>, <<"text/plain">>, []},
                                 Request),
                    ?assertMatch({ok, _}, Response)
            end()
    end,

    receive M2 ->
            fun () ->
                    {Request, Response, _ElapsedUs} = M2,
                    ?assertEqual({get, bucket(), <<"foo">>, []}, Request),
                    ?assertEqual({ok, <<"bar">>}, Response)
            end()
    end.

signed_url() ->
    s3:delete(bucket(), <<"foo">>, default_profile(), 5000),
    ?assertEqual({error, document_not_found},
                 s3:get(bucket(), <<"foo">>, [], default_profile(), 5000)),
    {ok, _} = s3:put(bucket(), <<"foo">>, <<"signed_test">>, <<"text/plain">>,
                     [], default_profile(), 5000),

    {MegaSeconds, Seconds, _} = os:timestamp(),
    Expires = MegaSeconds * 1000000 + Seconds,
    Url = s3:signed_url(bucket(), <<"foo">>, Expires + 3600, get, default_profile(), 5000),

    ?assertMatch(
       {ok, {{200, _}, _, <<"signed_test">>}},
       lhttpc:request(Url, get, [], [], 5000, [])
      ).

reload_config() ->
    OldConfig = s3_server:get_config(),
    s3_server:reload_config([{max_retries, 5}]),
    ?assertNotEqual(OldConfig, s3_server:get_config()).


%%
%% HELPERS
%%

very_slow_response(Module, Socket, _, _, _) ->
    timer:sleep(100),
    Module:send(
      Socket,
      "HTTP/1.1 200 OK\r\n"
      "Content-type: text/plain\r\nContent-length: 14\r\n\r\n"
      "Great success!"
    ).

delete_if_existing(Bucket, Key) ->
    case s3:get(Bucket, Key) of
        {ok, not_found} ->
            ok;
        {ok, _Doc} ->
            {ok, _} = s3:delete(Bucket, Key)
    end.

default_config() ->
    [{endpoint, "localhost:4567"} | credentials()].

default_profile() ->
    {<<"access_key">>, <<"secret_access_key">>, <<"localhost:4567">>}.

credentials() ->
    File = filename:join([code:priv_dir(s3erl2), "s3_credentials.term"]),
    case file:consult(File) of
        {ok, Cred} ->
            Cred;
        {error, enoent} ->
            throw({error, missing_s3_credentials})
    end.

bucket() ->
    File = filename:join([code:priv_dir(s3erl2), "bucket.term"]),
    {ok, Config} = file:consult(File),
    proplists:get_value(bucket, Config).
