(ns drift-db-h2.test.flavor
  (:use [drift-db-h2.flavor])
  (:use [clojure.test])
  (:require [drift-db.core :as drift-db]))

(def dbname "test")

(def db-dir "test/db/data/")

(deftest test-parse-column
  (is (= (parse-column { :default "NULL" :key "PRI" :null "NO" :type "VARCHAR(20)" :field "NAME" })
          { :default "NULL", :length 20, :not-null true, :primary-key true, :name :name, :type :string }))
  (is (= (parse-column { :default "NULL" :key "" :null "YES" :type "DATE(8)" :field "CREATED_AT"})
        { :default "NULL" :length 8 :name :created-at, :type :date })))

(deftest create-flavor
  (let [flavor (h2-flavor dbname db-dir)]
    (try
      (is flavor)
      (drift-db/init-flavor flavor)
      (drift-db/create-table :test
        (drift-db/string :name { :length 20 :not-null true :primary-key true })
        (drift-db/date :created-at))
      (is (drift-db/table-exists? :test))
      (is (= (drift-db/describe-table :test)
            { :name :test
              :columns [{ :default "NULL", :length 20, :not-null true, :primary-key true, :name :name, :type :string }
                        { :default "NULL" :length 8 :name :created-at, :type :date }] }))
      (finally 
        (drift-db/drop-table :test)
        (is (not (drift-db/table-exists? :test)))))))
