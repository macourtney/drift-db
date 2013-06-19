(ns drift-db-postgresql.test.column
  (:use [drift-db-postgresql.column])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column-protocol]
            [drift-db.core :as drift-db]))

(defn assert-name [column name]
  (is (= (when column (column-protocol/name column)) name)))

(defn assert-type [column type]
  (is (= (when column (column-protocol/type column)) type)))

(defn assert-length [column length]
  (is (= (when column (column-protocol/length column)) length)))

(defn assert-precision [column precision]
  (is (= (when column (column-protocol/precision column)) precision)))

(defn assert-scale [column scale]
  (is (= (when column (column-protocol/scale column)) scale)))

(defn assert-nullable? [column nullable?]
  (is (= (when column (column-protocol/nullable? column)) nullable?)))

(defn assert-primary-key? [column primary-key?]
  (is (= (when column (column-protocol/primary-key? column)) (or primary-key? false))))

(defn assert-column-map [column column-map]
  (assert-name column (:name column-map))
  (assert-type column (:type column-map))
  (assert-length column (:length column-map))
  (assert-precision column (:precision column-map))
  (assert-scale column (:scale column-map))
  (assert-nullable? column (not (:not-null column-map)))
  (assert-primary-key? column (:primary-key column-map)))

(deftest test-column-name
  (is (= (column-name :test) "\"test\""))
  (is (= (column-name (drift-db/string :test)) "\"test\"")))

(deftest test-parse-column
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "boolean" :column-name "is_active" })
                     { :name :is-active, :type :boolean })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "bytea" :column-name "data" })
                     { :name :data, :type :byte-array })
  (assert-column-map (parse-column { :default "NULL" :key "PRI" :is-nullable "NO" :data-type "character varying" :column-name "NAME" :character-maximum-length 20 })
                     { :length 20, :not-null true, :primary-key true, :name :name, :type :string })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "date" :column-name "CREATED_AT" })
                     { :name :created-at, :type :date })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "timestamp without time zone" :column-name "EDITED_AT" })
                     { :name :edited-at, :type :date-time })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "integer" :column-name "FOO" })
                     { :name :foo, :type :integer })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "numeric" :column-name "BAR" :numeric-scale 5 :numeric-precision 10 })
                     { :precision 10 :scale 5 :name :bar, :type :decimal })
  (assert-column-map (parse-column { :default "NULL", :key "", :data-type "text", :column-name "DESCRIPTION" })
                     { :name :description, :type :text })
  (assert-column-map (parse-column { :default "NULL" :key "" :data-type "time without time zone" :column-name "DELETED_AT" })
                     { :name :deleted-at, :type :time }))

(deftest test-db-type
  (is (= (db-type (drift-db/boolean :name)) "BOOLEAN"))
  (is (= (db-type (drift-db/byte-array :name)) "BYTEA"))
  (is (= (db-type (drift-db/string :name { :length 20 })) "VARCHAR(20)"))
  (is (= (db-type (drift-db/date :created-at)) "DATE"))
  (is (= (db-type (drift-db/date-time :edited-at)) "TIMESTAMP"))
  (is (= (db-type (drift-db/integer :foo { :length 9 })) "INTEGER"))
  (is (= (db-type (drift-db/id :id)) "SERIAL"))
  (is (= (db-type (drift-db/belongs-to :parent { :length 20 })) "BIGINT"))
  (is (= (db-type (drift-db/decimal :bar { :precision 5 :scale 10 })) "DECIMAL(5,10)"))
  (is (= (db-type (drift-db/text :notes)) "TEXT"))
  (is (= (db-type (drift-db/time-type :deleted-at)) "TIME")))

(deftest test-type-vec
  (is (= (type-vec (drift-db/integer :foo { :length 9 })) ["INTEGER"]))
  (is (= (type-vec (drift-db/id :id)) ["SERIAL"])))

(deftest test-column-spec-vec
  (is (= (column-spec-vec (drift-db/integer :foo)) ["\"foo\"" "INTEGER"])))

(deftest test-spec-vec
  (is (= (spec-vec (drift-db/integer :foo)) ["\"foo\"" "INTEGER"])))

(deftest test-spec-str
  (is (= (spec-str (drift-db/integer :foo)) "\"foo\" INTEGER")))