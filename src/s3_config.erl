%%
%% S3 config
%%
%% Responds to get_credentials
-module(s3_config).

-behaviour(gen_server).

-include("s3.hrl").

%% API
-export([start_link/1, get_credentials/0, get_endpoint/0, get_retry_callback/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(config, {
          access_key,
          secret_access_key,
          max_sessions = 10,
          max_pipeline_size = 10,
          endpoint,
          retry_callback
}).


%% API

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

get_credentials() ->
    gen_server:call(?MODULE, get_credentials).

get_endpoint() ->
    gen_server:call(?MODULE, get_endpoint).

get_retry_callback() ->
    gen_server:call(?MODULE, get_retry_callback).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    AccessKey       = proplists:get_value(access_key, Config),
    SecretAccessKey = proplists:get_value(secret_access_key, Config),
    MaxSessions     = proplists:get_value(max_sessions, Config),
    MaxPipelineSize = proplists:get_value(max_pipeline_size, Config),
    Endpoint        = proplists:get_value(endpoint, Config),
    RetryCallback   = proplists:get_value(retry_callback, Config, fun (_, _) -> ok end),

    {ok, #config{access_key        = AccessKey,
                 secret_access_key = SecretAccessKey,
                 max_sessions      = MaxSessions,
                 max_pipeline_size = MaxPipelineSize,
                 endpoint          = Endpoint,
                 retry_callback    = RetryCallback
                }}.

handle_call(get_credentials, _From, State) ->
    {reply, {ok, {State#config.access_key, State#config.secret_access_key}}, State};

handle_call(get_endpoint, _From, State) ->
    {reply, {ok, State#config.endpoint}, State};

handle_call(get_retry_callback, _From, State) ->
    {reply, {ok, State#config.retry_callback}, State}.

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
