-type bucket()      :: string().
-type key()         :: string().
-type contenttype() :: string().
-type body()        :: binary().
-type etag()        :: string().

-record(config, {
          access_key,
          secret_access_key,
          timeout,
          retry_callback,
          max_retries,
          retry_delay
}).
