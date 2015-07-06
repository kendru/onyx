(ns onyx.compression.nippy
  (:require [taoensso.nippy :as nippy]))

(defn compress [x]
  (nippy/freeze x {:compressor nil}))

(defn decompress [x]
  (nippy/thaw x {:v1-compatibility? false}))

; (defrecord LogEntry [fn args])

; (nippy/extend-freeze LogEntry :onyx/le ; A unique (namespaced) type identifier
;                      [x data-output]
;                      (nippy/freeze-to-out! data-output (:fn x))
;                      (nippy/freeze-to-out! data-output (:args x)))

; (nippy/extend-thaw :onyx/le ; Same type id
;                    [data-input]
;                    (->LogEntry (nippy/thaw-from-in! data-input)
;                                (nippy/thaw-from-in! data-input)))


 ;(alength (compress (->LogEntry 1 2)))
; (alength (compress {:fn :a :args :b} ))
