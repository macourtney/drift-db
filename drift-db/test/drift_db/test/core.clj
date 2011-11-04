(ns drift-db.test.core
  (:use [drift-db.core])
  (:use [clojure.test]))

(deftest test-date
  (is (= (date :test)
        { :spec-type :column
          :type :date
          :name :test }))
  (is (= (date :test { :fail :fail })
        { :spec-type :column
          :type :date
          :name :test })))
