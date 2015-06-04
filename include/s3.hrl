-type bucket()      :: binary().
-type key()         :: binary().
-type contenttype() :: binary().
-type value()       :: binary().
-type etag()        :: binary().
-type header()      :: {binary(), binary()}.
-type headers()     :: [header()].

-type profile_name() :: atom().
-type access_key() :: binary().
-type secret_access_key() :: binary().
-type endpoint() :: binary().
-type profile() :: profile_name() | {access_key(), secret_access_key(), endpoint()}.

-type error_reason() :: any().

-record(config, {
          timeout,
          retry_callback,
          max_retries,
          retry_delay,
          max_concurrency,
          max_concurrency_cb,
          post_request_cb,
          return_headers
}).
