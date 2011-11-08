(ns drift-db-h2.test.flavor
  (:use [drift-db-h2.flavor])
  (:use [clojure.test]))

(def dbname "test")

(def db-dir "test/db/data/")

(deftest create-flavor
  (is (h2-flavor dbname db-dir)))
