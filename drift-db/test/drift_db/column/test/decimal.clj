(ns drift-db.column.test.decimal
  (:use [drift-db.column.decimal])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.spec :as spec]))

(deftest test-type
  (let [name :test
        precision 10
        scale 5
        not-null true
        primary-key true
        column (create-column name { :precision precision :scale scale :not-null not-null :primary-key primary-key })]
    (is (= (spec/type column) :column))
    (is (= (column-protocol/type column) :decimal))
    (is (= (column-protocol/name column) name))
    (is (= (column-protocol/length column) nil))
    (is (= (column-protocol/precision column) precision))
    (is (= (column-protocol/scale column) scale))
    (is (= (column-protocol/nullable? column) (not not-null)))
    (is (= (column-protocol/primary-key? column) primary-key))
    (is (not (column-protocol/auto-increment? column)))))