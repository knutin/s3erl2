-module(s3_test).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "s3erl-test").


integration_test_() ->
    {foreach,
     fun setup/0,
     fun teardown/1,
     [
      ?_test(get_put()),
      ?_test(concurrency_limit()),
      ?_test(timeout_retry()),
      ?_test(slow_endpoint())
     ]}.

setup() ->
    application:start(crypto),
    application:start(public_key),
    application:start(ssl),
    application:start(lhttpc),
    {ok, Pid} = s3_server:start_link(default_config()),
    [Pid].

teardown(Pids) ->
    [begin unlink(P), exit(P, kill) end || P <- Pids].




get_put() ->
    delete_if_existing(?BUCKET, <<"foo">>),

    ?assertEqual({ok, not_found}, s3:get(?BUCKET, <<"foo">>)),
    ?assertEqual({ok, not_found}, s3:get(?BUCKET, "foo")),

    ?assertMatch({ok, _}, s3:put(?BUCKET, <<"foo">>, <<"bazbar">>, "text/plain")),
    {ok, <<"bazbar">>} = s3:get(?BUCKET, <<"foo">>).


concurrency_limit() ->
    Parent = self(),
    MaxConcurrencyCB = fun (N) -> Parent ! {max_concurrency, N} end,

    s3_server:stop(),
    s3_server:start_link([{max_concurrency_callback, MaxConcurrencyCB},
                          {max_concurrency, 3}] ++ default_config()),

    meck:new(s3_lib),
    GetF = fun (_, _, _) -> timer:sleep(50), {ok, <<"bazbar">>} end,
    meck:expect(s3_lib, get, GetF),
    meck:expect(s3_lib, get, GetF),
    meck:expect(s3_lib, get, GetF),


    P1 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P2 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P3 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P4 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),

    receive M0 -> ?assertEqual({max_concurrency, 3}, M0) end,
    receive {P1, M1} -> ?assertEqual({ok, <<"bazbar">>}, M1) end,
    receive {P2, M2} -> ?assertEqual({ok, <<"bazbar">>}, M2) end,
    receive {P3, M3} -> ?assertEqual({ok, <<"bazbar">>}, M3) end,
    receive {P4, M4} -> ?assertEqual({error, max_concurrency}, M4) end,

    ?assertEqual({ok, <<"bazbar">>}, s3:get(?BUCKET, <<"foo">>)),

    meck:validate(s3_lib),
    meck:unload(s3_lib).

timeout_retry() ->
    Parent = self(),
    RetryCb = fun (Reason, Attempt) ->
                      Parent ! {Reason, Attempt}
              end,
    s3_server:stop(),
    s3_server:start_link([{timeout, 10},
                          {retry_callback, RetryCb},
                          {retry_delay, 10}] ++ default_config()),

    meck:new(lhttpc),
    TimeoutF = fun (_, _, _, _, _) ->
                       timer:sleep(50),
                       {error, timeout}
               end,
    meck:expect(lhttpc, request, TimeoutF),
    %% meck:expect(lhttpc, request, TimeoutF),
    %% meck:expect(lhttpc, request, TimeoutF),

    ?assertEqual({error, timeout}, s3:get(?BUCKET, <<"foo">>)),

    receive M1 -> ?assertEqual({timeout, 0}, M1) end,
    receive M2 -> ?assertEqual({timeout, 1}, M2) end,
    receive M3 -> ?assertEqual({timeout, 2}, M3) end,
    receive _  -> ?assert(false) after 1 -> ok end,

    ?assert(meck:validate(lhttpc)),
    meck:unload(lhttpc).

slow_endpoint() ->
    Port = webserver:start(gen_tcp, [fun very_slow_response/5]),

    s3_server:stop(),
    s3_server:start_link([{timeout, 10},
                          {endpoint, "localhost:" ++ integer_to_list(Port)},
                          {retry_delay, 10}] ++ default_config()),

    ?assertEqual({error, timeout}, s3:get(?BUCKET, <<"foo">>, 100)).


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
            s3:delete(Bucket, Key)
    end.

default_config() ->
    credentials().

credentials() ->
    File = filename:join([code:priv_dir(s3erl), "s3_credentials.term"]),
    {ok, Cred} = file:consult(File),
    Cred.
