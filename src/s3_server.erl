%% gen_server wrapping calls to s3. Contains configuration in state,
%% isolates caller from library failure.
-module(s3_server).
-behaviour(gen_server).
-include("s3.hrl").

%% API
-export([start_link/1, get/2, put/4, delete/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {config}).

%%
%% API
%%

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

get(Bucket, Key) ->
    get(Bucket, Key, 5000).
get(Bucket, Key, Timeout) ->
    gen_server:call(?MODULE, {get, Bucket, Key}, Timeout).

put(Bucket, Key, Value, ContentType) ->
    put(Bucket, Key, Value, ContentType, 5000).
put(Bucket, Key, Value, ContentType, Timeout) ->
    gen_server:call(?MODULE, {put, Bucket, Key, Value, ContentType}, Timeout).

delete(Bucket, Key) ->
    gen_server:call(?MODULE, {delete, Bucket, Key}, 5000).


%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    AccessKey       = proplists:get_value(access_key, Config),
    SecretAccessKey = proplists:get_value(secret_access_key, Config),
    Timeout         = proplists:get_value(timeout, Config, 1500),
    RetryCallback   = proplists:get_value(retry_callback, Config,
                                          fun (_, _) -> ok end),
    MaxRetries      = proplists:get_value(max_retries, Config, 3),
    RetryDelay      = proplists:get_value(retry_delay, Config, 500),

    {ok, #state{config = #config{access_key        = AccessKey,
                                 secret_access_key = SecretAccessKey,
                                 timeout           = Timeout,
                                 retry_callback    = RetryCallback,
                                 max_retries       = MaxRetries,
                                 retry_delay       = RetryDelay
                                }}}.

handle_call({get, Bucket, Key}, _From, #state{config = C} = State) ->
    {reply, s3:get(C, Bucket, Key), State};

handle_call({put, Bucket, Key, Value, ContentType}, _From,
            #state{config = C} = State) ->
    {reply, s3:put(C, Bucket, Key, Value, ContentType), State};

handle_call({delete, Bucket, Key}, _From, #state{config = C} = State) ->
    {reply, s3:delete(C, Bucket, Key), State}.


handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

