{application, s3erl,
 [
  {description, "s3erl app"},
  {vsn, "0.1.0"},
  {registered, []},
  {modules, [s3, s3app, s3pool, s3pool_sup, s3util,s3test]},
  {applications, [kernel,
                  stdlib,
                  inets,
                  ibrowse
                 ]},
  {mod, {s3app, []}},
  {env, [{retries, 10},{retry_delay, 10},{timeout, 20000},{worker, 50}]}
 ]}.
