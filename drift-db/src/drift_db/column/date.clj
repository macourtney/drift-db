(ns drift-db.column.date
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype DateColumn [name]
  column/Column
  (type [column]
    :date)

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

(extend-type DateColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (DateColumn. name)))