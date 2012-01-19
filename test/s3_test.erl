-module(s3_test).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "s3erl-test").


integration_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      ?_test(get_put()),
      ?_test(concurrency_limit())
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

    ?assertEqual({error, not_found}, s3:get(?BUCKET, <<"foo">>)),
    ?assertEqual({error, not_found}, s3:get(?BUCKET, "foo")),

    ?assertMatch({ok, _}, s3:put(?BUCKET, <<"foo">>, <<"bazbar">>, "text/plain")),
    {ok, <<"bazbar">>} = s3:get(?BUCKET, <<"foo">>).


concurrency_limit() ->
    meck:new(s3_lib),
    GetF = fun (_, _, _) -> timer:sleep(50), {ok, <<"bazbar">>} end,
    meck:expect(s3_lib, get, GetF),
    meck:expect(s3_lib, get, GetF),
    meck:expect(s3_lib, get, GetF),

    Parent = self(),
    P1 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P2 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P3 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),
    P4 = spawn(fun() -> Parent ! {self(), s3:get(?BUCKET, <<"foo">>)} end),

    receive {P1, M1} -> ?assertEqual({ok, <<"bazbar">>}, M1) end,
    receive {P2, M2} -> ?assertEqual({ok, <<"bazbar">>}, M2) end,
    receive {P3, M3} -> ?assertEqual({ok, <<"bazbar">>}, M3) end,
    receive {P4, M4} -> ?assertEqual({error, max_concurrency}, M4) end,

    ?assertEqual({ok, <<"bazbar">>}, s3:get(?BUCKET, <<"foo">>)),

    meck:validate(s3_lib),
    meck:unload(s3_lib).




%%
%% HELPERS
%%

delete_if_existing(Bucket, Key) ->
    case s3:get(Bucket, Key) of
        {error, not_found} ->
            ok;
        {ok, _Doc} ->
            s3:delete(Bucket, Key)
    end.

default_config() ->
    [{max_concurrency, 3}] ++ credentials().

credentials() ->
    File = filename:join([code:priv_dir(s3erl), "s3_credentials.term"]),
    {ok, Cred} = file:consult(File),
    Cred.
