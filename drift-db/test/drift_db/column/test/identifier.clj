(ns drift-db.column.test.identifier
  (:use [drift-db.column.identifier])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.spec :as spec]))

(deftest test-type
  (let [name :test
        column (create-column name)]
    (is (= (spec/type column) :column))
    (is (= (column-protocol/type column) :id))
    (is (= (column-protocol/name column) name))
    (is (nil? (column-protocol/length column)))
    (is (nil? (column-protocol/precision column)))
    (is (nil? (column-protocol/scale column)))
    (is (not (column-protocol/nullable? column)))
    (is (column-protocol/primary-key? column))
    (is (column-protocol/auto-increment? column))))