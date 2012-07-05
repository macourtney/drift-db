(ns drift-db.column.belongs-to
  (:require [clojure.tools.logging :as logging]
            [clojure.tools.loading-utils :as loading-utils]
            [clojure.tools.string-utils :as string-utils]
            [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(deftype BelongsToColumn [name length nullable? primary-key? auto-increment?]
  column/Column
  (type [column]
    :belongs-to)

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

(extend-type BelongsToColumn
  spec/Spec
  (type [column] :column))

(defn create-name [model]
  (string-utils/add-ending-if-absent (loading-utils/dashes-to-underscores (name model)) "_id"))

(defn create-column
  ([model]
    (BelongsToColumn. (create-name model) nil nil nil nil))
  ([model mods]
    (BelongsToColumn. (create-name model) (:length mods) (not (:not-null mods)) (or (:primary-key mods) false)
                      (:auto-increment mods))))