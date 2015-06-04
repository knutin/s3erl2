# S3, version 2

NOTE: This is work in progress! Use at your own risk!

This is an Erlang client for Amazons Simple Storage Service, aka
S3. It is based on the much forked s3erl library, originally written
by Andrew Birkett <andy@nobugs.org>.

This particular version has a few features which we found to be very
useful in busier systems where reliability is key.

 * Safe retries, your request will suceed or fail within the time you
   specified, with the user process able to continue execution
 * Concurrency is limited to a configurable number of concurrent
   requests to S3, in which case calls return immediately. This allows
   the user to decide how to handle overload by for example shedding
   load, instead of trashing the HTTP client.
 * Binaries for keys and values
 * Instrumentation, with callbacks after every request, retry or
   concurrency limit reached.
 * Configurable endpoint, which means you can use `fakes3` for
   testing.
 * Configurable credentials and endpoint at request time, which means
   you can use different credentials for different endpoints or
   buckets.

NOTE: `s3erl2` is not API-compatible with `s3erl`.


## Usage

Currently, you must add the `s3_server` to your own supervisor tree,
as `s3erl` does not include it's own supervisor.

```erlang
s3_spec() ->
    Options =
        [{timeout, 15000},
         {max_concurrency, 500},
         {retry_callback, fun my_module:s3_retry_callback/2},
         {post_request_callback, fun my_module:s3_post_request_callback/3}],

    {s3, {s3_server, start_link, [Options]},
     permanent, 5000, worker, []}.
```

You can of course also start `s3_server` from the shell for
experimenting, like this:
`s3_server:start_link([])`

Then you can start using S3:
```erlang
1> s3:get(<<"my-bucket">>, <<"my-key">>).
{error, document_not_found}
2> s3:put(<<"my-bucket">>, <<"my-key">>, <<"big binary blob">>, <<"text/plain">>).
{ok, <<"etag">>}
3> s3:get(<<"my-bucket">>, <<"my-key">>).
{ok, <<"big binary blob">>}
```

See the test suite in `test/s3_test.erl` for more examples of usage.


## Timeouts

Care has been taken to make sure requests can be retried in a timely
and safe manner. If you're doing a significant amount of requests to
S3, you will see many requests failing, most of which returns
correctly with another attempt.

The behaviour of retries is completely up to the user. `s3erl` allows
you to configure up front how many times you want to retry, the
timeout for each request, the delay between requests and a callback
function called after every failed request to help you measure and log
failed requests.

With the options below, each request from a client process
(ie. `s3:get/3`) is allowed 3 failed requests before it returns with
an error. Each request may either return immediately with an error
(ie. 500 from S3) or is considered to have timed out after 1000
ms. After each request, there's a think time of 500 ms before retrying
the request. The total time `s3erl` will spend on this request is then
`1000 * 3 + 500 * 3 = 4500`. When you factor in some slack, a client
timeout of 5000 ms in the `s3:get/3` is suitable.


```erlang
[{max_retries, 3},
 {timeout, 1000},
 {retry_delay, 500}]
```

## Running the tests

To run the unit tests, you need to use your own S3 credentials. These
should be placed in `priv/s3_credentials.term` according to the
template in `priv/s3_credentials.term.template`. In `priv/bucket.term`
you can configure which bucket to use. Run the tests with `./rebar
skip_deps=true eunit`.
