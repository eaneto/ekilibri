# Ekilibri

Ekilibri is, firstly, a learning project! It is an HTTP 1.1 server and
load balancer.

## TODO

- Use timeouts in the configuration file in the routed calls. Now
  they are only being used in the health check process.
- Enhance HTTP parsing implementation, parse headers over 4kB, lower
  case headers, support chunked transfer, support all methods.
- Enhance error handling making sure the server won't crash.
- Write better tests cases, the ones written right now(can be found at
  `tests` directory) only check basic things and there are only a few
  crash scenarios.

## Features

### Balancing Strategies

It has two simple balancing strategies.

#### Round Robin

In the round robin strategy ekilibri will pick a random server from
the servers in the configuration.

#### Least Connections

Ekilibri keeps track of all current requests happening to each server,
in this strategy ekilibri will _try_ to pick the server that is
currently handling less requests.

### Health Checks

Ekilibri has a background job to check if the servers are healthy, the
unhealthy servers will be removed as candidates to receive requests
until they can be considered healthy again. For that you need to
configure the `fail_window`, `max_fails` and `health_check_path`
properties, they are documented at [CLI](#cli)(there's also more
documentation explaining how the process works with more detail in the
code, this documentation can be generated with `cargo doc`).

## Integration tests

The integration tests are written in python with [pytest][1]. You can
install the dependencies with pip: `pip install -r
tests/requirements.txt`. And to execute all tests you can run:

```shell
pytest tests
```

## CLI

Right now ekilibri only has one option, the `-f` or `--file` to point
at the configuration file, by default it will look for a file named
`ekilibri.toml` in the directory executing the binary.

The configuration file follows this structure:
```toml
# List of the server addresses
servers = [
    "127.0.0.1:8081",
    "127.0.0.1:8082",
    "127.0.0.1:8083"
]

# Load balancing strategy.
strategy = "RoundRobin" # Or LeastConnections

# Maximum number of failed requests(non 200 responses and
# timeouts) allowed before a server is taken from the healthy
# servers list. A server that is not on this list won't receive
# any requests. The health check process runs once every 500ms.
max_fails = 5

# The time window to consider the [Config::max_fails] and remove
# a server from the healthy servers list, the time windows is
# relevant so that "old" requests don't interfere in the
# decision (in seconds).
fail_window = 60

# Timeout to establish a connection to one of the servers (in
# milliseconds).
connection_timeout = 1000

# Timeout writing the data to the server (in milliseconds).
write_timeout = 1000

# Timeout reading the data to the server (in milliseconds).
read_timeout = 1000

# The path to check the server's health. Ex.: "/health".
health_check_path = "/health"
```

## Problems

The current implementation of the HTTP protocol lacks almost
everything to make this actually useful. Not only that but also the
parts that it implements are not following the HTTP specification to
the letter. I don't intend to write a complete HTTP
server(implementing RFC 2616 in its entirety), but I do want to
implement correctly the parts that I'll use.
