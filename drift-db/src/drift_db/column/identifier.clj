(ns drift-db.column.identifier
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype IdentifierColumn [name]
  column/Column
  (type [column]
    :id)

  (name [column]
    name)

  (length [column]
    nil)

  (precision [column]
    nil)

  (scale [column]
    nil)

  (nullable? [column]
    false)

  (primary-key? [column]
    true)

  (auto-increment? [column]
    true))

(extend-type IdentifierColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (IdentifierColumn. name)))