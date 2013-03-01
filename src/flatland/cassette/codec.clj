(ns flatland.cassette.codec
  (:require [gloss.core :as gloss :refer [compile-frame finite-frame]]
            [gloss.io :as io]
            [gloss.core.codecs :as codecs]
            [gloss.data.bytes.core :as bytes]
            [gloss.core.protocols :refer [Reader Writer read-bytes write-bytes]]
            [flatland.useful.io :refer [mmap-file]]
            [me.raynes.fs :as fs])
  (:import java.util.zip.CRC32
           (java.nio ByteBuffer)
           (java.io StringReader)
           (java.util Collections Scanner)))

(defn minimum-size-finite-frame
  "Acts like gloss's finite-frame, but if the frame's length is less than the provided minimum, it
  consumes no bytes (not even the length prefix) and returns ::invalid."
  [min-size header-frame body-frame]
  (let [header-codec (compile-frame header-frame)
        body-codec (compile-frame body-frame)
        ordinary-codec (finite-frame header-codec body-codec)]
    (reify
      Reader
      (read-bytes [this bufseq]
        (let [[success x remainder] (read-bytes header-codec bufseq)]
          (cond (not success) [false this bufseq]
                (< x min-size) [true ::invalid bufseq]
                :else (-> (finite-frame x body-codec)
                          (read-bytes remainder)))))
      Writer
      (sizeof [this] nil)
      (write-bytes [this b v]
        (write-bytes ordinary-codec b v)))))

(defn len [^ByteBuffer buf]
  (- (.limit buf) (.position buf)))

(defn compute-crc [buf-seq]
  (let [crc (CRC32.)]
    (doseq [^ByteBuffer buf buf-seq]
      (let [arr (byte-array (len buf))]
        (.get (.duplicate buf) arr (.position buf) (.limit buf))
        (.update crc arr)))
    (.getValue crc)))

(defn wrap-crc [codec]
  (compile-frame [:uint32 codecs/identity-codec]
                 (fn add [val]
                   (let [encoded (io/encode codec val)]
                     [(compute-crc encoded) encoded]))
                 (fn check [[crc bytes]]
                   (if (= crc (compute-crc bytes))
                     (io/decode codec bytes)
                     ::invalid))))

(let [magic-byte (byte 0)]
  (defn message-codec [codec]
    (compile-frame (minimum-size-finite-frame 5 :uint32 [:byte (wrap-crc codec)])
                   (fn add [val]
                     [magic-byte val])
                   (fn check [[magic val]]
                     (if (= magic magic-byte)
                       val
                       ::invalid)))))

(defn kafka-file
  ([{:keys [path]}]
     (kafka-file {:path path} 99999999999))
  ([{:keys [path]} byte-offset]
     (let [files (.listFiles path)
           expected-name (format "%011d.kafka" byte-offset)
           acceptable-files (remove #(neg? (compare expected-name (.getName %)))
                                    files)]
       (Collections/max acceptable-files))))

(defn first-offset [filename]
  (let [scanner (doto (Scanner. (StringReader. filename))
                  (.useDelimiter #"\."))]
    (.nextLong scanner)))

(defn read-one [bufseq codec]
  (let [len (bytes/byte-count bufseq)
        [success x remainder] (read-bytes codec bufseq)]
    (if (and success (not= x ::invalid))
      {:success true, :value x, :len (- len (bytes/byte-count remainder))}
      {:success false})))

(defn read-message [{:keys [path codec]} byte-offset]
  (let [file (kafka-file path)
        offset (first-offset (fs/name file))
        {:keys [^ByteBuffer buffer close]} (mmap-file file)]
    (.position buffer (- byte-offset first-offset))
    (read-one buffer codec)))

(defn read-messages [{codec :codec, {buffer :buffer} :handle, :as topic}]
  (lazy-seq
    (let [dup (.slice buffer)
          {:keys [success value len]} (read-one (bytes/create-buf-seq dup) codec)]
      (when success
        (.position buffer (+ (.position buffer) len))
        (cons value (read-messages topic))))))
