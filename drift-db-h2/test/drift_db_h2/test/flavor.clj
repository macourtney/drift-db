(ns drift-db-h2.test.flavor
  (:use [drift-db-h2.flavor])
  (:use [clojure.test])
  (:require [drift-db.core :as drift-db]))

(def dbname "test")

(def db-dir "test/db/data/")

(deftest create-flavor
  (let [flavor (h2-flavor dbname db-dir)]
    (try
      (is flavor)
      (drift-db/init-flavor flavor)
      (drift-db/create-table :test
        (drift-db/string :name { :type :string :length 20 :not-null true :primary-key true }))
      (is (drift-db/table-exists? :test))
      (is (= (drift-db/describe-table :test)
            { :name :test :columns [{:name "NAME" :type :string :length 20 :not-null true :primary-key true }]}))
      (finally 
        (drift-db/drop-table :test)
        (is (not (drift-db/table-exists? :test)))))))
