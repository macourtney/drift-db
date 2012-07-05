(ns drift-db.column.string
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype StringColumn [name length nullable? primary-key?]
  column/Column
  (type [column]
    :string)

  (name [column]
    name)

  (length [column]
    length)

  (precision [column]
    nil)

  (scale [column]
    nil)

  (nullable? [column]
    nullable?)

  (primary-key? [column]
    primary-key?)

  (auto-increment? [column]
    false))

(extend-type StringColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (StringColumn. name nil nil nil))
  ([name mods]
    (StringColumn. name (:length mods) (not (:not-null mods)) (or (:primary-key mods) false))))