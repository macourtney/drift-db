(ns drift-db-h2.test.flavor
  (:use [drift-db-h2.flavor])
  (:use [clojure.test])
  (:require [drift-db.core :as drift-db])
  (:import [java.util Date]))

(def dbname "test")

(def db-dir "test/db/data/")

(deftest test-parse-column
  (is (= (parse-column { :default "NULL" :key "PRI" :null "NO" :type "VARCHAR(20)" :field "NAME" })
          { :default "NULL", :length 20, :not-null true, :primary-key true, :name :name, :type :string }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "DATE(8)" :field "CREATED_AT"})
        { :default "NULL" :length 8 :name :created-at, :type :date }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "TIMESTAMP(23)" :field "EDITED_AT"})
        { :default "NULL" :length 23 :name :edited-at, :type :date-time }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "INTEGER(10)" :field "FOO"})
        { :default "NULL" :length 10 :name :foo, :type :integer }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "DECIMAL(10,10)" :field "BAR"})
        { :default "NULL" :precision 10 :scale 10 :name :bar, :type :decimal }))
  (is (= (parse-column { :default "NULL", :key "", :null "YES", :type "CLOB(2147483647)", :field "DESCRIPTION" })
        { :default "NULL" :length 2147483647 :name :description, :type :text }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "TIME(6)" :field "DELETED_AT"})
        { :default "NULL" :length 6 :name :deleted-at, :type :time })))

(defn is-column? [column column-name]
  (when (= (:name column) column-name)
    column))

(defn find-column [table-description column-name]
  (some #(is-column? %1 column-name) (:columns table-description)))

(deftest create-flavor
  (let [flavor (h2-flavor dbname db-dir)]
    (try
      (is flavor)
      (drift-db/init-flavor flavor)
      (drift-db/create-table :test
        (drift-db/string :name { :length 20 :not-null true :primary-key true })
        (drift-db/date :created-at)
        (drift-db/date-time :edited-at)
        (drift-db/integer :foo)
        (drift-db/decimal :bar)
        (drift-db/text :description)
        (drift-db/time-type :deleted-at))
      (is (drift-db/table-exists? :test))
      (is (= (drift-db/describe-table :test)
            { :name :test
              :columns [{ :default "NULL", :length 20, :not-null true, :primary-key true, :name :name, :type :string }
                        { :default "NULL" :length 8 :name :created-at, :type :date }
                        { :default "NULL" :length 23 :name :edited-at, :type :date-time }
                        { :default "NULL" :length 10 :name :foo, :type :integer }
                        { :default "NULL" :precision 20 :name :bar, :type :decimal }
                        { :default "NULL" :length 2147483647 :name :description, :type :text }
                        { :default "NULL" :length 6 :name :deleted-at, :type :time }] }))
      (drift-db/add-column :test
        (drift-db/string :added))
      (is (= (find-column (drift-db/describe-table :test) :added)
            { :default "NULL", :length 255, :name :added, :type :string }))
      (drift-db/drop-column :test :added)
      (is (nil? (find-column (drift-db/describe-table :test) :added)))
      
      (finally 
        (drift-db/drop-table :test)
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
        (is (= (first (drift-db/execute-query ["SELECT * FROM TEST WHERE NAME = ?" test-row-name])) test-row))
        (drift-db/update :test ["NAME = ?" test-row-name] { :name test-row-name2 })
        (is (= (first (drift-db/execute-query ["SELECT * FROM TEST WHERE NAME = ?" test-row-name2])) test-row2))
        (drift-db/delete :test ["NAME = ?" test-row-name2])
        (is (nil? (first (drift-db/execute-query ["SELECT * FROM TEST WHERE NAME = ?" test-row-name2])))))
      (finally 
        (drift-db/drop-table :test)
        (is (not (drift-db/table-exists? :test)))))))