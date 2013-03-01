(ns flatland.cassette.util)

(defn kafka-file
  "Takes a byte offset and returns a kafka filename with a proper name for
   that offset."
  [offset]
  (format "%011d.kafka" offset))
