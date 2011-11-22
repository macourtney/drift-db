(ns drift-db-mysql.test.flavor
  (:use drift-db-mysql.flavor
        clojure.test)
  (:require [drift-db.core :as drift-db]))

(def dbname "drift_db_test")

(def username "drift-db")
(def password "drift-db-pass")

(deftest create-flavor
  (let [flavor (mysql-flavor username password dbname)]
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
              :columns [{ :default "" :length 20 :not-null true :primary-key true :name :name :type :string }
                        { :name :created-at :type :date }
                        { :name :edited-at :type :date-time }
                        { :length 11 :name :foo :type :integer }
                        { :scale 6 :precision 20 :name :bar :type :decimal }
                        { :name :description :type :text }
                        { :name :deleted-at :type :time }] }))
      (is (drift-db/column-exists? :test :foo))
      (is (drift-db/column-exists? :test "bar"))
      (drift-db/add-column :test
        (drift-db/string :added))
      (is (= (drift-db/find-column :test :added)
            { :length 255, :name :added, :type :string }))
      (drift-db/drop-column :test :added)
      (is (not (drift-db/column-exists? :test :added)))

      (drift-db/drop-column-if-exists :test :added)

      (drift-db/drop-column-if-exists :test :bar)
      (is (not (drift-db/column-exists? :test :bar)))

      (finally 
        (drift-db/drop-table :test)
        (is (not (drift-db/table-exists? :test)))))))

(deftest test-rows
  (let [flavor (mysql-flavor username password dbname)]
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
