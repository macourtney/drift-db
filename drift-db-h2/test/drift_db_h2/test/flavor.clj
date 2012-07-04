(ns drift-db-h2.test.flavor
  (:use [drift-db-h2.flavor])
  (:use [clojure.test])
  (:require [drift-db.core :as drift-db])
  (:import [java.util Date]))

(def dbname "test")

(def db-dir "test/db/data/")

(deftest test-parse-column
  (is (= (parse-column { :default "NULL" :key "PRI" :null "NO" :type "VARCHAR(20)" :field "NAME" })
          { :length 20, :not-null true, :primary-key true, :name :name, :type :string }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "DATE(8)" :field "CREATED_AT"})
        { :length 8 :name :created-at, :type :date }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "TIMESTAMP(23)" :field "EDITED_AT"})
        { :length 23 :name :edited-at, :type :date-time }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "INTEGER(10)" :field "FOO"})
        { :length 10 :name :foo, :type :integer }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "DECIMAL(10,10)" :field "BAR"})
        { :precision 10 :scale 10 :name :bar, :type :decimal }))
  (is (= (parse-column { :default "NULL", :key "", :null "YES", :type "CLOB(2147483647)", :field "DESCRIPTION" })
        { :length 2147483647 :name :description, :type :text }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "TIME(6)" :field "DELETED_AT"})
        { :length 6 :name :deleted-at, :type :time })))

(deftest create-flavor
  (let [flavor (h2-flavor dbname db-dir)]
    (try
      (is flavor)
      (drift-db/init-flavor flavor)
      (drift-db/create-table :test
        (drift-db/integer :id { :auto-increment true :primary-key true })
        (drift-db/string :name { :length 20 :not-null true })
        (drift-db/date :created-at)
        (drift-db/date-time :edited-at)
        (drift-db/decimal :bar)
        (drift-db/text :description)
        (drift-db/time-type :deleted-at))
      (is (drift-db/table-exists? :test))
      (let [table-description (drift-db/describe-table :test)
            expected-columns [{ :auto-increment true :length 10 :not-null true :name :id, :type :integer :primary-key true }
                              { :length 20, :not-null true, :name :name, :type :string }
                              { :length 8 :name :created-at, :type :date }
                              { :length 23 :name :edited-at, :type :date-time }
                              { :precision 20 :name :bar, :type :decimal }
                              { :length 2147483647 :name :description, :type :text }
                              { :length 6 :name :deleted-at, :type :time }]]
        (is (= (get table-description :name) :test))
        (is (get table-description :columns))
        (is (= (count (get table-description :columns)) (count expected-columns)))
        (doseq [column-pair (map #(list %1 %2) (get table-description :columns) expected-columns)]
          (is (= (dissoc (first column-pair) :default) (second column-pair)))))
      (is (drift-db/column-exists? :test :id))
      (is (drift-db/column-exists? :test "bar"))
      (drift-db/add-column :test
        (drift-db/string :added))
      (is (= (drift-db/find-column (drift-db/describe-table :test) :added)
            { :length 255, :name :added, :type :string }))
      (drift-db/update-column :test
        :added (drift-db/string :altered-test))
      (is (= (drift-db/find-column :test :altered-test)
            { :length 255, :name :altered-test, :type :string }))
      (drift-db/update-column :test
        :altered-test (drift-db/string :altered { :length 100 }))
      (is (= (drift-db/find-column (drift-db/describe-table :test) :altered)
            { :length 100, :name :altered, :type :string }))
      (drift-db/drop-column :test :altered)
      (is (not (drift-db/column-exists? :test :altered)))

      (drift-db/drop-column-if-exists :test :altered)
      (drift-db/drop-column-if-exists :test :bar)
      (is (not (drift-db/column-exists? :test :bar)))

      (finally 
        (drift-db/drop-table-if-exists :test)
        (is (not (drift-db/table-exists? :test)))))))

(deftest test-rows
  (let [flavor (h2-flavor dbname db-dir)]
    (try
      (is flavor)
      (drift-db/init-flavor flavor)
      (drift-db/create-table :test
        (drift-db/string :name { :length 20 :not-null true :primary-key true }))
      (is (drift-db/table-exists? :test))
      (let [test-row-name "blah"
            test-row-name2 "blah2"
            test-row { :name test-row-name }
            test-row2 { :name test-row-name2 }]
        (drift-db/insert-into :test test-row)
        (is (= (first (drift-db/sql-find { :table :test :where [(str "NAME = '" test-row-name "'")] :limit 1 })) test-row))
        (drift-db/update :test ["NAME = ?" test-row-name] { :name test-row-name2 })
        (is (= (first (drift-db/sql-find { :table :test :where ["NAME = ?" test-row-name2] })) test-row2))
        (drift-db/update :test { :name test-row-name2 } { :name test-row-name })
        (is (= (first (drift-db/sql-find { :table :test :where ["NAME = ?" test-row-name] })) test-row))
        (drift-db/delete :test ["NAME = ?" test-row-name])
        (is (nil? (first (drift-db/sql-find { :table :test :where { :name test-row-name } })))))
      (finally 
        (drift-db/drop-table :test)
        (is (not (drift-db/table-exists? :test)))))))