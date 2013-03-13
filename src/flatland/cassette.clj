(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame byte-count]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]]
            [flatland.useful.fn :refer [fix]]
            [me.raynes.fs :as fs]
            [flatland.cassette.util :refer [kafka-file]]
            [flatland.cassette.codec :as codec])
  (:import [java.io RandomAccessFile]))

(defn grow-file
  "Set the length of a random access file to n bytes. Should
   have the effect of creating a sparse file on Linux and Windows,
   but not OS X because HFS+ does not support this (apparently)."
  [^RandomAccessFile file n]
  (.setLength file n))

(defn space?
  "Check if buf has enough space to store the bytes from buf-seq in it."
  [buf buf-seq]
  (>= (- (.capacity buf) (.position buf))
      (byte-count buf-seq)))

(defn compute-file-name
  "Takes the byte offset where the current buffer stops and the name of the
   currently open file and with this information determines the name of the
   next file to roll over to."
  [byte-offset name]
  (kafka-file
   (if name
     (+ (Long/parseLong (fs/name name))
        byte-offset)
     0)))

(defn mmap
  "Memory map a file. If size is passed, grow the file to that size
   first."
  [name & [size]]
  (let [file (RandomAccessFile. name "rw")
        mapped (mmap-file (if size
                            (doto file (grow-file size))
                            file))]
    ((:close mapped))
    mapped))

(defn roll-over
  "If the topic has an open file already and rolls over to a new file.
   If this topic has no currently open file, creates the first one."
  [topic]
  (let [{:keys [path size handle name]} topic
        pos (when-let [buffer (:buffer handle)]
              (.position buffer))
        name (compute-file-name pos name)
        handle (mmap (fs/file path name) size)]
    (assoc topic :handle handle :name name)))

(defn get-buffer
  "Pull the raw memory mapped buffer out of a topic."
  [topic]
  (get-in topic [:handle :buffer]))

(defn append-message!
  "Append a message to the topic."
  [handle value]
  (let [encoded (encode (:codec @handle) value)
        buffer (get-buffer (swap! handle
                                  fix #(not (space? (get-buffer %) encoded))
                                  roll-over))]
    (doseq [buf encoded]
      (.put buffer buf))
    handle))

(defn advance-buffer!
  "Advance the topic's buffer to after the last written message. This is
   useful for positioning the buffer so that writes go to the end."
  [handle]
  (dorun (codec/read-messages @handle)))

(defn read-messages* [topic]
  (codec/read-messages (update-in topic [:handle :buffer] #(doto %
                                                             (.duplicate)
                                                             (.position 0)))))

(defn read-messages
  "Returns a lazy sequence of messages in this topic's currently open buffer."
  [handle]
  (read-messages* @handle))

(defn- coll-subseq
  "Colls should be a descending sequence of ascending sequences, like ((10 11 12) (5 6 8) (1 2 4)).
   Returns all items for which (pred x) is true, assuming (and (pred x) (< x y)) implies (pred y)."
  [pred colls]
  (loop [candidates (), colls colls]
    (if-let [[coll & colls] (seq colls)]
      (if (when-let [x (first coll)]
            (pred x))
        (recur (conj candidates coll) colls)
        (apply concat (drop-while (complement pred) coll)
               candidates))
      (apply concat candidates))))

(defn messages-since [handle pred]
  (let [topic @handle
        files (sort (comp - compare) (.listFiles (:path topic)))
        decoded-values (for [file files]
                         (read-messages* (assoc topic :handle (mmap file))))]
    (coll-subseq pred decoded-values)))

(defn open
  "Opens an existing topic directory for writing. Path is where
   the topics are stored and topic is the topic name itself. A
   topic map with a pre-advanced buffer will be returned and be
   immediately writable."
  [path topic codec]
  (let [topic-dir (fs/file path topic)
        {:keys [buffer close]} (mmap (codec/kafka-file {:path topic-dir}))]
    (doto (atom {:path topic-dir
                 :size (.capacity buffer)
                 :handle {:buffer buffer
                          :close close}
                 :codec (codec/message-codec codec)})
      (advance-buffer!))))

(def default-size 524288000)

(defn create
  "Create a new topic. path is the path where the topic will be created.
   topic is the name of the topic and the directory where the files
   associated with the topic will live. codec is the codec to encode
   payloads as. You can optionally provide size which is the maximum
   file size of each portion of the topic."
  ([path topic codec] (create path topic codec default-size))
  ([path topic codec size]
     (let [topic (fs/file path topic)]
       (fs/mkdirs topic)
       (atom (roll-over {:path topic
                         :codec (codec/message-codec codec)
                         :size (or size default-size)})))))

(defn create-or-open
  "Creates or opens a new topic depending on whether or not it exists."
  ([path topic codec] (create-or-open path topic codec default-size))
  ([path topic codec size]
     (if (fs/exists? (fs/file path topic))
       (open path topic codec)
       (create path topic codec size))))
