(ns drift-db.column.time
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype TimeColumn [name]
  column/Column
  (type [column]
    :time)

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

(extend-type TimeColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (TimeColumn. name)))