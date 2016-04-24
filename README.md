# Logstash Cassandra Output Plugin

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

It was originally a fork of the [logstash-output-cassandra](https://github.com/otokarev/logstash-output-cassandra) plugin by [Oleg Tokarev](https://github.com/otokarev), which has gone unmaintained and went through a major re-design in this version we built.

## Usage

<pre><code>
output {
    cassandra {
        # List of Cassandra hostname(s) or IP-address(es)
        hosts => [ "cass-01", "cass-02" ]

        # The port cassandra is listening to
        port => 9042

        # The protocol version to use with cassandra
        protocol_version => 4

        # Cassandra consistency level.
        # Options: "any", "one", "two", "three", "quorum", "all", "local_quorum", "each_quorum", "serial", "local_serial", "local_one"
        # Default: "one"
        consistency => 'any'

        # The keyspace to use
        keyspace => "a_ks"

        # The table to use (event level processing (e.g. %{[key]}) is supported)
        table => "%{[@metadata][cassandra_table]}"

        # Username
        username => "cassandra"

        # Password
        password => "cassandra"

        # An optional hints hash which will be used in case filter_transform or filter_transform_event_key are not in use
        # It is used to trigger a forced type casting to the cassandra driver types in
        # the form of a hash from column name to type name in the following manner:
        hints => {
            id => "int"
            at => "timestamp"
            resellerId => "int"
            errno => "int"
            duration => "float"
            ip => "inet"
        }

        # The retry policy to use (the default is the default retry policy)
        # the hash requires the name of the policy and the params it requires
        # The available policy names are:
        # * default => retry once if needed / possible
        # * downgrading_consistency => retry once with a best guess lowered consistency
        # * failthrough => fail immediately (i.e. no retries)
        # * backoff => a version of the default retry policy but with configurable backoff retries
        # The backoff options are as follows:
        # * backoff_type => either * or ** for linear and exponential backoffs respectively
        # * backoff_size => the left operand for the backoff type in seconds
        # * retry_limit => the maximum amount of retries to allow per query
        # example:
        # using { "type" => "backoff" "backoff_type" => "**" "backoff_size" => 2 "retry_limit" => 10 } will perform 10 retries with the following wait times: 1, 2, 4, 8, 16, ... 1024
        # NOTE: there is an underlying assumption that the insert query is idempotent !!!
        # NOTE: when the backoff retry policy is used, it will also be used to handle pure client timeouts and not just ones coming from the coordinator
        retry_policy => { "type" => "default" }

        # The command execution timeout
        request_timeout => 1

        # Ignore bad values
        ignore_bad_values => false

        # In Logstashes >= 2.2 this setting defines the maximum sized bulk request Logstash will make
        # You you may want to increase this to be in line with your pipeline's batch size.
        # If you specify a number larger than the batch size of your pipeline it will have no effect,
        # save for the case where a filter increases the size of an inflight batch by outputting
        # events.
        #
        # In Logstashes <= 2.1 this plugin uses its own internal buffer of events.
        # This config option sets that size. In these older logstashes this size may
        # have a significant impact on heap usage, whereas in 2.2+ it will never increase it.
        # To make efficient bulk API calls, we will buffer a certain number of
        # events before flushing that out to Cassandra. This setting
        # controls how many events will be buffered before sending a batch
        # of events. Increasing the `flush_size` has an effect on Logstash's heap size.
        # Remember to also increase the heap size using `LS_HEAP_SIZE` if you are sending big commands
        # or have increased the `flush_size` to a higher value.
        flush_size => 500

        # The amount of time since last flush before a flush is forced.
        #
        # This setting helps ensure slow event rates don't get stuck in Logstash.
        # For example, if your `flush_size` is 100, and you have received 10 events,
        # and it has been more than `idle_flush_time` seconds since the last flush,
        # Logstash will flush those 10 events automatically.
        #
        # This helps keep both fast and slow log streams moving along in
        # near-real-time.
        idle_flush_time => 1
    }
}
</code></pre>

## Running Plugin in Logstash
### Run in a local Logstash clone

Edit Logstash Gemfile and add the local plugin path, for example:
```
gem "logstash-output-cassandra", :path => "/your/local/logstash-output-cassandra"
```

Install plugin
```
bin/plugin install --no-verify
```
Run Logstash with the plugin
```
bin/logstash -e 'output {cassandra {}}'
```

### Run in an installed Logstash

You can use the same method to run your plugin in an installed Logstash by editing its Gemfile and pointing the :path to your local plugin development directory or you can build the gem and install it using:

Build your plugin gem
```
gem build logstash-output-cassandra.gemspec
```
Install the plugin from the Logstash home
```
bin/plugin install /your/local/plugin/logstash-output-cassandra.gem
```
Run Logstash with the plugin
```
bin/logstash -e 'output {cassandra {}}'
```

## TODO
* Finish integration specs
    * it "properly works with counter columns"
    * it "properly adds multiple events to multiple tables in the same bulk"
* Improve retries to include (but probably only handle Errors::Timeout and Errors::NoHostsAvailable):
    * \#get_query
    * \#execute_async
* Upgrade / test with logstash 2.3
* Upgrade / test with cassandra 3
