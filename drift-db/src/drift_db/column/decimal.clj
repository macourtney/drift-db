(ns drift-db.column.decimal
  (:require [clojure.tools.logging :as logging]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype DecimalColumn [name precision scale nullable? primary-key?]
  column/Column
  (type [column]
    :decimal)

  (name [column]
    name)

  (length [column]
    nil)

  (precision [column]
    precision)

  (scale [column]
    scale)

  (nullable? [column]
    nullable?)

  (primary-key? [column]
    primary-key?)

  (auto-increment? [column]
    false))

(extend-type DecimalColumn
  spec/Spec
  (type [column] :column))

(defn create-column
  ([name]
    (DecimalColumn. name nil nil nil nil))
  ([name mods]
    (DecimalColumn. name (:precision mods) (:scale mods) (not (:not-null mods)) (or (:primary-key mods) false))))