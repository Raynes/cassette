(ns flatland.cassette.codec
  (:require [gloss.core :as gloss :refer [compile-frame finite-frame]]
            [gloss.io :as io]
            [gloss.core.codecs :as codecs]
            [flatland.useful.io :refer [mmap-file]]
            [me.raynes.fs :as fs])
  (:import java.util.zip.CRC32
           (java.nio ByteBuffer)
           (java.io StringReader)
           (java.util Collections Scanner)))

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
                     (throw (IllegalArgumentException.
                             (format "CRC %d does not match: expected %d"
                                     crc, (compute-crc bytes))))))))

(let [magic-byte (byte 0)]
  (defn message-codec [codec]
    (compile-frame (finite-frame :uint32 [:byte (wrap-crc codec)])
                   (fn add [val]
                     [magic-byte val])
                   (fn check [[magic val]]
                     (if (= magic magic-byte)
                       val
                       (throw (IllegalArgumentException.
                               (format "Magic byte %d does not match: expected %d"
                                       magic magic-byte))))))))

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

(defn read-one [buffer codec]
  (let [[success x remainder] (read-bytes codec buffer)]
    (if success
      {:success true, :value x}
      {:success false})))

(defn read-message [{:keys [path codec]} byte-offset]
  (let [file (kafka-file path)
        offset (first-offset (fs/name file))
        {:keys [^ByteBuffer buffer close]} (mmap-file file)]
    (.position buffer (- byte-offset first-offset))
    (read-one buffer codec)))
