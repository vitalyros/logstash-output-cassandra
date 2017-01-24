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

        # Defines mapping of event data to a cassandra table.
        # It is required to set the the types of these cassandra columns.
        # Supports simple cassandra types, sets, lists and user defined types.
        # User defined types can be used only if they are defined in user_defined_types config.
        # It is possible to define the behavior in cases when event data is absent or invalid (impossible to transform to cassandra data type).
        # The config is an array of hashes with the following keys:
        # * name => mandatory string. Name of the cassandra column to save to.
        # * type => mandatory string. Cassandra type of the column.
        # * key => optional string. A pointer to saved event data. Either a top level event key, or a path to the data in [..][..].. notation.
        #     e.g. [top_level_key][child_key_1][child_key_2]
        #     if nil, a `name` parameter value will be used instead, [..][..].. notation will not be supported in this case
        # * on_nil => optional string. Default value is "fail".
        #     Defines behavior in the case when event data is absent (or nil) by the given event_key
        # * on_invalid => optional string. Default value is "fail".
        #     Defines behavior in the case when the data by given event_key can't be translated to the given cassandra_type
        #
        #     Both on_nil and on_invalid have the same form. Its one of the predefined strings:
        #     "fail" => an exception will be raised, consequently the event will not be saved to cassandra table.
        #     "ignore" => this column will either be skipped entirely (absent in an insert request to cassandra).
        #       This behavior can be used for optional columns, when it is unnecessary for incoming events to contain full set of data.
        #       This behavior can't be used for partitioning or clustering keys of the cassandra table, since they are mandatory.
        #     "ignore warn" => same behavior, as for "ignore", but a detailed warning message will be logged
        #     "default" => the value, set in the "default" field wfieldill be used.
        #       If the "default" field is not set, the default value will be chosen automatically, depending on the cassandra_type.
        #     "default warn" => same behavior, as for "default", but a detailed warning message will be logged.
        #
        # * default => optional string. Undefined by default. Default value used when either on_nil, or on_invalid is set to 'default'
        columns => [
            { name => "id" type => "text" },
            { key => "[data][comment]" name => "comment" type => "text" on_nil => "ignore" on_invalid => "ignore warn"},
            { key => "[data][amount]" name => "amount" type => "int" on_nil => "default" default => "0"}.
            { name => "user_data" type => "user_data" }
        ]

        # Defines types that can be used in `columns` configuration
        # These types are intended to map a hash field that is part of an event to the value of a cassandra user defined type.
        # It is possible to define the behavior in cases when event data is absent or invalid (impossible to transform to cassandra data type)
        # NOTE: in runtime, if after the transformation and application of on_nil / on_invalid behavior the resulting user defined type value is an empty hash - will raise an error.
        # The config structure:
        # * name => mandatory string. Name of the type as it can be used in `columns` configuration. May not correspond to the name of the cassandra user defined type.
        # * fields => an array of hashes, mandatory, at least one item is required.
        #   Can contain following key => value pairs:
        #   * name => mandatory string. Name of the cassandra column to save to.
        #   * type => mandatory string. Cassandra type of the column.
        #   * key => optional string. A pointer to saved event data. Either a top level event key, or a path to the data in [..][..].. notation.
        #       e.g. [top_level_key][child_key_1][child_key_2]
        #       if nil, a `name` parameter value will be used instead, [..][..].. notation will not be supported in this case
        # * on_nil => optional string,
        #     Defines behavior in the case when event data is absent (or nil) by the given event_key
        # * on_invalid => optional string. Default value is "fail".
        #     Defines behavior in the case when the data by given event_key can't be translated to the given cassandra_type
        #
        #     See columns config for possible values of on_nil and on_invalid
        #
        user_defined_types => [
            { 
                name => "mytype" 
                fields => [
                    { key => "some_text" type => "text" },
                    { key => "some_int" type => "int" },
                    { key => "some_text_again" type => "text" on_nil => "ignore" }
                ]
            }
        ]
        
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
And install by executing:
```
bin/plugin install --no-verify
```

Or install plugin from RubyGems:
```
bin/plugin install logstash-output-cassandra
```

And then run Logstash with the plugin:
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
