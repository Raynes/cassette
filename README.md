# cassette

Cassette is a Clojure implementation of
[Apache Kafka's](http://kafka.apache.org/). It is a compact format suitable for
representing a stream of events, log entries, etc.

Cassette makes use of [gloss](https://github.com/ztellman/gloss), and expects
that the format you use to store your data with will be a gloss codec.

## Usage

Here is a simple repl session:

```clojure
user=> (require '[flatland.cassette :as c])
nil
user=> (require '[gloss.core :as g])
nil
user=> (def codec (g/string :utf8))
#'user/codec
user=> codec
#<structure$compile_frame$reify__2688 gloss.core.structure$compile_frame$reify__2688@3114fb83>
user=> (def topic (c/create-or-open "topics" "my-topic" codec))
#'user/topic
user=> topic
{:name "00000000000.kafka", :handle {:buffer #<DirectByteBuffer java.nio.DirectByteBuffer[pos=0 lim=524288000 cap=524288000]>, :close #<io$mmap_file$close__8585 flatland.useful.io$mmap_file$close__8585@2772d440>}, :path #<File /Users/raynes/code/cassette/topics/my-topic>, :codec #<structure$compile_frame$reify__2688 gloss.core.structure$compile_frame$reify__2688@63ccc9fb>, :size 524288000}
user=> (c/append-message! topic "hi!")
{:name "00000000000.kafka", :handle {:buffer #<DirectByteBuffer java.nio.DirectByteBuffer[pos=12 lim=524288000 cap=524288000]>, :close #<io$mmap_file$close__8585 flatland.useful.io$mmap_file$close__8585@2772d440>}, :path #<File /Users/raynes/code/cassette/topics/my-topic>, :codec #<structure$compile_frame$reify__2688 gloss.core.structure$compile_frame$reify__2688@63ccc9fb>, :size 524288000}
user=> (c/append-message! topic "there!")
{:name "00000000000.kafka", :handle {:buffer #<DirectByteBuffer java.nio.DirectByteBuffer[pos=27 lim=524288000 cap=524288000]>, :close #<io$mmap_file$close__8585 flatland.useful.io$mmap_file$close__8585@2772d440>}, :path #<File /Users/raynes/code/cassette/topics/my-topic>, :codec #<structure$compile_frame$reify__2688 gloss.core.structure$compile_frame$reify__2688@63ccc9fb>, :size 524288000}
user=> (c/read-messages topic)
("hi!" "there!")
```

Cassette works by creating a 'topic' directory for you and giving you a file to
start with. Eventually as you append to this file it'll automatically roll over
to a new file at a (configurable) partition point.

## License

Distributed under the Eclipse Public License, the same as Clojure.
