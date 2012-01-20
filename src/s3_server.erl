%% @doc gen_server wrapping calls to s3. Contains configuration in
%% state, isolates caller from library failure, controls concurrency,
%% manages retries.
-module(s3_server).
-behaviour(gen_server).

-include("s3.hrl").

%% API
-export([start_link/1, get_num_workers/0, get_stats/0, stop/0]).
-export([default_max_concurrency_cb/1, default_retry_cb/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {config, workers, reqs_processed}).

%%
%% API
%%

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

get_num_workers() ->
    gen_server:call(?MODULE, get_num_workers).

get_stats() ->
    gen_server:call(?MODULE, get_stats).

stop() ->
    gen_server:call(?MODULE, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    process_flag(trap_exit, true),

    AccessKey        = v(access_key, Config),
    SecretAccessKey  = v(secret_access_key, Config),
    Endpoint         = v(endpoint, Config),

    Timeout          = v(timeout, Config, 1500),
    RetryCallback    = v(retry_callback, Config,
                         fun ?MODULE:default_retry_cb/2),
    MaxRetries       = v(max_retries, Config, 3),
    RetryDelay       = v(retry_delay, Config, 500),
    MaxConcurrency   = v(max_concurrency, Config, 50),
    MaxConcurrencyCB = v(max_concurrency_callback, Config,
                         fun ?MODULE:default_max_concurrency_cb/1),

    C = #config{access_key         = AccessKey,
                secret_access_key  = SecretAccessKey,
                endpoint           = Endpoint,
                timeout            = Timeout,
                retry_callback     = RetryCallback,
                max_retries        = MaxRetries,
                retry_delay        = RetryDelay,
                max_concurrency    = MaxConcurrency,
                max_concurrency_cb = MaxConcurrencyCB},

    {ok, #state{config = C, workers = [], reqs_processed = 0}}.

handle_call({request, Req}, From, #state{config = C} = State)
  when length(State#state.workers) < C#config.max_concurrency ->
    WorkerPid =
        spawn_link(fun() ->
                           gen_server:reply(From, handle_request(Req, C))
                   end),
    {noreply, State#state{workers = [WorkerPid | State#state.workers]}};

handle_call({request, _}, _From, #state{config = C} = State)
  when length(State#state.workers) >= C#config.max_concurrency ->
    (C#config.max_concurrency_cb)(C#config.max_concurrency),
    {reply, {error, max_concurrency}, State};

handle_call(get_num_workers, _From, #state{workers = Workers} = State) ->
    {reply, length(Workers), State};

handle_call(get_stats, _From, State) ->
    {reply, {ok, [{reqs_processed, State#state.reqs_processed}]}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, normal}, State) ->
    case lists:member(Pid, State#state.workers) of
        true ->
            NewWorkers = lists:delete(Pid, State#state.workers),
            {noreply,
             State#state{workers = NewWorkers,
                         reqs_processed = State#state.reqs_processed +1}};
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

%% @doc: Executes the given request, will retry if request failed
handle_request(Req, C) ->
    handle_request(Req, C, 0).

handle_request(Req, C, Attempts) ->
    case catch execute_request(Req, C) of
        %% Stop retrying if we hit max retries
        {error, E} when Attempts =:= C#config.max_retries ->
            {error, E};
        {'EXIT', {econnrefused, _}} when Attempts =:= C#config.max_retries ->
            {error, econnrefused};

        %% Continue trying if we have connection related errors
        {error, Reason} when Reason =:= connect_timeout orelse
                             Reason =:= timeout ->
            (C#config.retry_callback)(Reason, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);
        {'EXIT', {econnrefused, _}} ->
            error_logger:info_msg("exit: ~p~n", [{Req, Attempts}]),
            (C#config.retry_callback)(econnrefused, Attempts),
            timer:sleep(C#config.retry_delay),
            handle_request(Req, C, Attempts + 1);

        %% Success!
        Response when element(1, Response) =:= ok ->
            Response
    end.

execute_request({get, Bucket, Key}, C) ->
    s3_lib:get(C, Bucket, Key);
execute_request({put, Bucket, Key, Value, ContentType}, C) ->
    s3_lib:put(C, Bucket, Key, Value, ContentType);
execute_request({delete, Bucket, Key}, C) ->
    s3_lib:delete(C, Bucket, Key).

default_max_concurrency_cb(_) -> ok.
default_retry_cb(_, _) -> ok.

