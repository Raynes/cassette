(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame byte-count]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]]
            [me.raynes.fs :as fs])
  (:import [java.io RandomAccessFile]))

(defn message-codec [codec]
  (finite-frame :int32 codec))

(defn grow-file [^RandomAccessFile file size]
  ;; Should create a sparse file on Linux and Windows, but apparently not OS X (HFS+).
  (.setLength file size))

(defn sort-topic
  "Sort all of the files in a topic."
  [path topic]
  (->> (fs/file path topic)
       (fs/list-dir)
       (filter fs/file?)
       (sort)))

(defn space? [buf buf-seq]
  (> (- (.capacity buf) (.position buf))
     (byte-count buf-seq)))

(defn compute-file-name 
  [old size] 
  (if old
    (str (+ (Long. (fs/name old)) size) ".kafka")
    "00000000000.kafka"))

(defn roll-over
  "Rolls over to a new file (or starts a new topic). Closes
   the previous memory mapped file (if there was one)."
  [top]
  (let [{:keys [path size current]} top
        name (compute-file-name (:name current) size)
        buffer (mmap-file
                (doto (RandomAccessFile.
                       (fs/file path name)
                       "rw")
                  (grow-file size)))]
    (when-let [closer (get-in current [:buffer :close])]
      (closer))
    (assoc top :current {:buffer buffer
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
