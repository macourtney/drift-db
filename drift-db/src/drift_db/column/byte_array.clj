(ns drift-db.column.byte-array
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype ByteArrayColumn [name]
  column/Column
  (type [column]
    :byte-array)

  (name [column]
    name)

  (length [column]
    nil)

  (precision [column]
    nil)

  (scale [column]
    nil)

  (nullable? [column]
    true)

  (primary-key? [column]
    false)

  (auto-increment? [column]
    false))

(extend-type ByteArrayColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name] (ByteArrayColumn. name)))