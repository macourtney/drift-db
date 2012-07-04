(ns drift-db-postgresql.test.flavor
  (:use drift-db-postgresql.flavor
        clojure.test)
  (:require [drift-db.core :as drift-db]))

(def dbname "drift_db_test")

(def username "drift-db")
(def password "drift-db-pass")

(deftest create-flavor
  (let [flavor (postgresql-flavor username password dbname)]
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
            expected-columns [{ :name :id :not-null true :type :integer :auto-increment true }
                              { :name :name :length 20 :not-null true :type :string }
                              { :name :created-at :type :date }
                              { :name :edited-at :type :date-time }
                              { :name :bar :scale 6 :precision 20 :type :decimal }
                              { :name :description :type :text }
                              { :name :deleted-at :type :time }]]
        (is (= (get table-description :name) :test))
        (is (get table-description :columns))
        (is (= (count (get table-description :columns)) (count expected-columns)))
        (doseq [column-pair (map #(list %1 %2) (get table-description :columns) (reverse expected-columns))]
          (is (= (first column-pair) (second column-pair)))))
      (is (drift-db/column-exists? :test :id))
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
        (drift-db/drop-table-if-exists :test)
        (is (not (drift-db/table-exists? :test)))))))

(deftest test-rows
  (let [flavor (postgresql-flavor username password dbname)]
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
