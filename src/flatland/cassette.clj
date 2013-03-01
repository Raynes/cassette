(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame byte-count]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]]
            [me.raynes.fs :as fs]
            [flatland.cassette.codec :refer [message-codec]])
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

(defn kafka-file
  "Takes a byte offset and returns a kafka filename with a proper name for
   that offset."
  [offset]
  (format "%011d.kafka" offset))

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

(defn roll-over
  "If the topic has an open file already, rolls over to a new file and closes
   the previous memory mapped file. If this topic has no currently open file,
   creates the first one."
  [topic]
  (let [{:keys [path size handle name]} topic
        pos (when-let [buffer (:buffer handle)]
              (.position buffer))
        name (compute-file-name pos name)
        handle (mmap-file
                (doto (RandomAccessFile.
                       (fs/file path name)
                       "rw")
                  (grow-file size)))]
    (when-let [closer (:close handle)]
      (closer))
    (assoc topic :handle handle :name name)))

(defn get-buffer
  "Pull the raw memory mapped buffer out of a topic."
  [topic]
  (get-in topic [:handle :buffer]))

(defn append-message!
  "Append a message to the topic."
  [topic value]
  (let [encoded (encode (message-codec (:codec topic)) value)
        topic (if (space? (get-buffer topic) encoded)
                topic
                (roll-over topic))]
    (doseq [buf encoded]
      (.put (get-buffer topic) buf))
    topic))

(defn read-messages
  ([frame buffer] (read-messages frame buffer (.position buffer)))
  ([frame buffer offset]
     (let [codec (compile-frame (message-codec frame))]
       (.position buffer offset)
       (take-while (complement nil?) (lazy-decode-all codec buffer)))))

(defn create
  "Create a new topic. path is the path where the topic will be created.
   topic is the name of the topic and the directory where the files
   associated with the topic will live. codec is the codec to encode
   payloads as. You can optionally provide size which is the maximum
   file size of each portion of the topic."
  ([path topic codec] (create path topic codec 524288000))
  ([path topic codec size]
     (let [topic (fs/file path topic)]
       (fs/mkdirs topic)
       (roll-over {:path topic
                   :codec (compile-frame codec)
                   :size size}))))
