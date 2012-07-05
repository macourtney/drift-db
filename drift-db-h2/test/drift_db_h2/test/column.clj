(ns drift-db-h2.test.column
  (:use [drift-db-h2.column])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.core :as drift-db]))

(defn assert-name [column name]
  (is (= (column-protocol/name column) name)))

(defn assert-type [column type]
  (is (= (column-protocol/type column) type)))

(defn assert-length [column length]
  (is (= (column-protocol/length column) length)))

(defn assert-precision [column precision]
  (is (= (column-protocol/precision column) precision)))

(defn assert-scale [column scale]
  (is (= (column-protocol/scale column) scale)))

(defn assert-nullable? [column nullable?]
  (is (= (column-protocol/nullable? column) nullable?)))

(defn assert-primary-key? [column primary-key?]
  (is (= (column-protocol/primary-key? column) (or primary-key? false))))

(defn assert-column-map [column column-map]
  (assert-name column (:name column-map))
  (assert-type column (:type column-map))
  (assert-length column (:length column-map))
  (assert-precision column (:precision column-map))
  (assert-scale column (:scale column-map))
  (assert-nullable? column (not (:not-null column-map)))
  (assert-primary-key? column (:primary-key column-map)))

(deftest test-parse-column
  (assert-column-map (parse-column { :default "NULL" :key "PRI" :null "NO" :type "VARCHAR(20)" :field "NAME" })
                     { :length 20, :not-null true, :primary-key true, :name :name, :type :string })
  (assert-column-map (parse-column { :default "NULL" :key "" :null "YES" :type "DATE(8)" :field "CREATED_AT"})
                     { :name :created-at, :type :date })
  (assert-column-map (parse-column { :default "NULL" :key "" :null "YES" :type "TIMESTAMP(23)" :field "EDITED_AT"})
                     { :name :edited-at, :type :date-time })
  (assert-column-map (parse-column { :default "NULL" :key "" :null "YES" :type "INTEGER(10)" :field "FOO"})
                     { :length 10 :name :foo, :type :integer })
  (assert-column-map (parse-column { :default "NULL" :key "" :null "YES" :type "DECIMAL(10,10)" :field "BAR"})
                     { :precision 10 :scale 10 :name :bar, :type :decimal })
  (assert-column-map (parse-column { :default "NULL", :key "", :null "YES", :type "CLOB(2147483647)", :field "DESCRIPTION" })
                     { :name :description, :type :text })
  (assert-column-map (parse-column { :default "NULL" :key "" :null "YES" :type "TIME(6)" :field "DELETED_AT"})
                     { :name :deleted-at, :type :time }))

(deftest test-db-type
  (is (= (db-type (drift-db/string :name { :length 20 })) "VARCHAR(20)"))
  (is (= (db-type (drift-db/date :created-at)) "DATE"))
  (is (= (db-type (drift-db/date-time :edited-at)) "DATETIME"))
  (is (= (db-type (drift-db/integer :foo { :length 9 })) "INT"))
  (is (= (db-type (drift-db/id :id)) "INT"))
  (is (= (db-type (drift-db/belongs-to :parent { :length 20 })) "BIGINT"))
  (is (= (db-type (drift-db/decimal :bar { :precision 5 :scale 10 })) "DECIMAL(5,10)"))
  (is (= (db-type (drift-db/text :notes)) "TEXT"))
  (is (= (db-type (drift-db/time-type :deleted-at)) "TIME")))

(deftest test-type-vec
  (is (= (type-vec (drift-db/integer :foo { :length 9 })) ["INT"]))
  (is (= (type-vec (drift-db/id :id)) ["INT" "NOT NULL" "AUTO_INCREMENT" "PRIMARY KEY"])))

(deftest test-column-spec-vec
  (is (= (column-spec-vec (drift-db/integer :foo)) ["foo" "INT"])))

(deftest test-spec-vec
  (is (= (spec-vec (drift-db/integer :foo)) ["foo" "INT"])))

(deftest test-spec-str
  (is (= (spec-str (drift-db/integer :foo)) "foo INT")))