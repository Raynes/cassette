(ns flatland.cassette.codec
  (:require [gloss.core.protocols :refer [Reader Writer]]
            [gloss.core :refer [finite-block finite-frame defcodec]]
            [gloss.io :refer [encode decode]]
            [gloss.data.bytes.core :refer [duplicate take-bytes drop-bytes create-buf-seq]]
            [gloss.core.formats :only [to-buf-seq]])
  (:import java.util.zip.CRC32))

(defn message-codec [codec]
  (finite-frame :int32 [:byte :uint32 codec]))

(defn len [buf]
  (- (.limit buf) (.position buf)))

(defn compute-crc [buf-seq]
  (let [crc (CRC32.)]
    (doseq [buf buf-seq]
      (let [arr (byte-array (len buf))]
        (.get (duplicate buf) arr (.position buf) (.limit buf))
        (.update crc arr)))
    (.getValue crc)))

(defcodec crc :uint32)

(defn get-crc [buf-seq]
  (let [buf-seq (create-buf-seq buf-seq)]
    (decode crc (drop-bytes (take-bytes buf-seq 9) 5))))

(defn get-body [buf-seq]
  (let [buf-seq (create-buf-seq buf-seq)]
    (drop-bytes buf-seq 9)))

(defn compare-crc [buf-seq]
  (let [buf-seq (create-buf-seq buf-seq)]
    (= (get-crc buf-seq)
       (-> buf-seq get-body compute-crc))))
