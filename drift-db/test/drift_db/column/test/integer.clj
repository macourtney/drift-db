(ns drift-db.column.test.integer
  (:use [drift-db.column.integer])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.spec :as spec]))

(deftest test-type
  (let [name :test
        length 100
        not-null true
        primary-key true
        auto-increment true
        column (create-column name { :length length :not-null not-null :primary-key primary-key :auto-increment auto-increment })]
    (is (= (spec/type column) :column))
    (is (= (column-protocol/type column) :integer))
    (is (= (column-protocol/name column) name))
    (is (= (column-protocol/length column) length))
    (is (nil? (column-protocol/precision column)))
    (is (nil? (column-protocol/scale column)))
    (is (= (column-protocol/nullable? column) (not not-null)))
    (is (= (column-protocol/primary-key? column) primary-key))
    (is (= (column-protocol/auto-increment? column) auto-increment))))