(ns drift-db.column.integer
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype IntegerColumn [name length nullable? primary-key? auto-increment?]
  column/Column
  (type [column]
    :integer)

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
    auto-increment?))

(extend-type IntegerColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (IntegerColumn. name nil true false false))
  ([name mods]
    (IntegerColumn. name (:length mods) (not (:not-null mods)) (or (:primary-key mods) false) (or (:auto-increment mods) false))))