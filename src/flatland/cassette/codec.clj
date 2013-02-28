(ns flatland.cassette.codec
  (:require [gloss.core :as gloss :refer [compile-frame finite-frame]]
            [gloss.io :as io]
            [gloss.core.codecs :as codecs])
  (:import java.util.zip.CRC32
           (java.nio ByteBuffer)))

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
                             (format "CRC %l does not match: expected %l"
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
