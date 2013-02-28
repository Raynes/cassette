(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame byte-count]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]]
            [me.raynes.fs :as fs]
            [flatland.cassette.codec :refer [message-codec]])
  (:import [java.io RandomAccessFile]))

(defn grow-file [^RandomAccessFile file size]
  ;; Should create a sparse file on Linux and Windows, but apparently not OS X (HFS+).
  (.setLength file size))

(defn space? [buf buf-seq]
  (>= (- (.capacity buf) (.position buf))
     (byte-count buf-seq)))

(defn compute-file-name 
  [byte-offset name]
  (format "%011d.kafka"
          (if name
            (+ (Long/parseLong (fs/name name))
               byte-offset)
            0)))

(defn roll-over
  "Rolls over to a new file (or begins the topic if no files exist).
   Closes the previous memory mapped file (if there was one)."
  [top]
  (let [{:keys [path size current]} top
        pos (when-let [buffer (get-in current [:handle :buffer])]
              (.position buffer))
        name (compute-file-name pos (:name current))
        handle (mmap-file
                (doto (RandomAccessFile.
                       (fs/file path name)
                       "rw")
                  (grow-file size)))]
    (when-let [closer (get-in current [:handle :close])]
      (closer))
    (assoc top :current {:handle handle
                         :name name})))

#_(defn append-message!
  [frame topic value]
  (let [encoded (encode (compile-frame (message-codec frame)) value)]
    (if (space? (:current topic) encoded)
      (doseq [buf encoded]
        (.put buffer buf))
      (recur (roll-over topic)))))

(defn read-messages
  ([frame buffer] (read-messages frame buffer (.position buffer)))
  ([frame buffer offset]
     (let [codec (compile-frame (message-codec frame))]
       (.position buffer offset)
       (take-while (complement nil?) (lazy-decode-all codec buffer)))))

(defn create
  "Create a new log file."
  ([path topic] (create path topic 524288000))
  ([path topic size]
     (let [topic (fs/file path topic)]
       (fs/mkdirs topic)
       (roll-over {:path topic
                   :size size}))))
