(ns drift-db.column.text
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype TextColumn [name]
  column/Column
  (type [column]
    :text)

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

(extend-type TextColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (TextColumn. name)))