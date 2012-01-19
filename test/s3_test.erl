-module(s3_test).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "s3erl-test").


integration_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [?_test(get_put())]}.

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



%%
%% HELPERS
%%

delete_if_existing(Bucket, Key) ->
    case s3_server:get(Bucket, Key) of
        {error, not_found} ->
            ok;
        {ok, _Doc} ->
            s3_server:delete(Bucket, Key)
    end.

default_config() ->
    credentials().

credentials() ->
    File = filename:join([code:priv_dir(s3erl), "s3_credentials.term"]),
    {ok, Cred} = file:consult(File),
    Cred.
