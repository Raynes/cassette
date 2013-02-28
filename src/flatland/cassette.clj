(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]])
  (:import [java.io RandomAccessFile]))

(defn message-codec [codec]
  (finite-frame :int32 codec))

(defn grow-file [^RandomAccessFile file size]
  ;; Should create a sparse file on Linux and Windows, but apparently not OS X (HFS+).
  (.setLength file size))

(defn write-message!
  ([frame buffer value]
     (write-message! frame buffer (.position buffer) value))
  ([frame buffer offset value]
     (let [encoded (encode (compile-frame (message-codec frame)) value)]
       (.position buffer offset)
       (doseq [buf encoded]
         (.put buffer buf))
       buffer)))

(defn read-messages
  ([frame buffer] (read-messages frame buffer (.position buffer)))
  ([frame buffer offset]
     (let [codec (compile-frame (message-codec frame))]
       (.position buffer offset)
       (take-while (complement nil?) (lazy-decode-all codec buffer)))))

(defn create
  "Create a new log file."
  ([path] (create path 524288000))
  ([path size]
     (mmap-file
      (doto (RandomAccessFile. path "rw")
        (grow-file size)))))
