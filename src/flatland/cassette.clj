(ns flatland.cassette
  (:require [gloss.core :refer [compile-frame finite-frame byte-count]]
            [gloss.core.protocols :refer [write-bytes]]
            [gloss.io :refer [lazy-decode-all encode]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.io :refer [mmap-file]]
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

(defn close
  "Closes the file associated with a topic."
  [topic]
  (when-let [closer (get-in topic [:handle :close])]
    (closer)))

(defn mmap
  "Memory map a file. If size is passed, grow the file to that size
   first."
  [name & [size]]
  (let [file (RandomAccessFile. name "rw")]
    (mmap-file (if size
                 (doto file (grow-file size))
                 file))))

(defn roll-over
  "If the topic has an open file already, rolls over to a new file and closes
   the previous memory mapped file. If this topic has no currently open file,
   creates the first one."
  [topic]
  (let [{:keys [path size handle name]} topic
        pos (when-let [buffer (:buffer handle)]
              (.position buffer))
        name (compute-file-name pos name)
        handle (mmap (fs/file path name) size)]
    (doto (assoc topic :handle handle :name name)
      (close))))

(defn get-buffer
  "Pull the raw memory mapped buffer out of a topic."
  [topic]
  (get-in topic [:handle :buffer]))

(defn append-message!
  "Append a message to the topic."
  [topic value]
  (let [encoded (encode (:codec topic) value)
        topic (if (space? (get-buffer topic) encoded)
                topic
                (roll-over topic))]
    (doseq [buf encoded]
      (.put (get-buffer topic) buf))
    topic))

(defn advance-buffer!
  "Advance the topic's buffer to after the last written message. This is
   useful for positioning the buffer so that writes go to the end."
  [topic]
  (dorun (codec/read-messages topic)))

(defn read-messages
  "Returns a lazy sequence of messages in this topic's currently open buffer."
  [topic]
  (codec/read-messages (update-in topic [:handle :buffer] #(doto %
                                                             (.duplicate)
                                                             (.position 0)))))

(defn open
  "Opens an existing topic directory for writing. Path is where
   the topics are stored and topic is the topic name itself. A
   topic map with a pre-advanced buffer will be returned and be
   immediately writable."
  [path topic codec]
  (let [topic-dir (fs/file path topic)
        {:keys [buffer close]} (mmap (codec/kafka-file {:path topic-dir}))]
    (doto {:path topic-dir
           :size (.capacity buffer)
           :handle {:buffer buffer
                    :close close}
           :codec (codec/message-codec codec)}
      (advance-buffer!))))

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
                   :codec (codec/message-codec codec)
                   :size size}))))
