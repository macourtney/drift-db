(ns drift-db.column.boolean
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype BooleanColumn [name]
  column/Column
  (type [column]
    :boolean)

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

(extend-type BooleanColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (BooleanColumn. name)))