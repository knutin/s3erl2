%% @doc gen_server wrapping calls to s3. Contains configuration in
%% state, isolates caller from library failure, controls concurrency,
%% manages retries.
-module(s3_server).
-behaviour(gen_server).

-include("s3.hrl").

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {config, workers}).

%%
%% API
%%

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).


%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    AccessKey       = v(access_key, Config),
    SecretAccessKey = v(secret_access_key, Config),

    Timeout         = v(timeout, Config, 1500),
    RetryCallback   = v(retry_callback, Config,
                        fun (_, _) -> ok end),
    MaxRetries      = v(max_retries, Config, 3),
    RetryDelay      = v(retry_delay, Config, 500),
    MaxConcurrency  = v(max_concurrency, Config, 50),

    C = #config{access_key        = AccessKey,
                secret_access_key = SecretAccessKey,
                timeout           = Timeout,
                retry_callback    = RetryCallback,
                max_retries       = MaxRetries,
                retry_delay       = RetryDelay,
                max_concurrency   = MaxConcurrency},

    {ok, #state{config = C, workers = []}}.


handle_call({request, Req}, From, #state{config = C} = State)
  when length(State#state.workers) < C#config.max_concurrency ->
    WorkerPid =
        spawn_link(fun() ->
                           gen_server:reply(From, handle_request(Req, C))
                   end),
    erlang:monitor(process, WorkerPid),
    {noreply, State#state{workers = [WorkerPid | State#state.workers]}};

handle_call({request, _}, _From, #state{config = C} = State)
  when length(State#state.workers) >= C#config.max_concurrency ->
    {reply, {error, max_concurrency}, State}.



handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, normal}, State) ->
    case lists:member(Pid, State#state.workers) of
        true ->
            {noreply, State#state{workers = lists:delete(Pid, State#state.workers)}};
        false ->
            error_logger:info_msg("ignored down message~n"),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    error_logger:info_msg("~p~n", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

v(Key, Data) ->
    proplists:get_value(Key, Data).

v(Key, Data, Default) ->
    proplists:get_value(Key, Data, Default).


handle_request(Req, C) ->
    execute_request(Req, C).

execute_request({get, Bucket, Key}, C) ->
    s3_lib:get(C, Bucket, Key);
execute_request({put, Bucket, Key, Value, ContentType}, C) ->
    s3_lib:put(C, Bucket, Key, Value, ContentType);
execute_request({delete, Bucket, Key}, C) ->
    s3_lib:delete(C, Bucket, Key).
