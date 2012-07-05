(ns drift-db.column.test.string
  (:use [drift-db.column.string])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.spec :as spec]))

(deftest test-type
  (let [name :test
        length 100
        not-null true
        primary-key true
        column (create-column name { :length length :not-null not-null :primary-key true })]
    (is (= (spec/type column) :column))
    (is (= (column-protocol/type column) :string))
    (is (= (column-protocol/name column) name))
    (is (= (column-protocol/length column) length))
    (is (nil? (column-protocol/precision column)))
    (is (nil? (column-protocol/scale column)))
    (is (= (column-protocol/nullable? column) (not not-null)))
    (is (= (column-protocol/primary-key? column) primary-key))
    (is (not (column-protocol/auto-increment? column)))))