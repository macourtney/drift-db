(ns drift-db.test.protocol
  (:use [drift-db.protocol])
  (:use [clojure.test]))

(deftype TestFlavor []
  Flavor
  (db-map [flavor]
    :db-map)
  (execute-query [flavor _]
    :execute-query)
  (execute-commands [flavor _]
    :execute-commands)
  (update [flavor _ _ _]
    :update)
  (insert-into [flavor _ _]
    :insert-into)
  (table-exists? [flavor _]
    :table-exists?)
  (sql-find [flavor _]
    :sql-find)
  (create-table [flavor _ _]
    :create-table)
  (drop-table [flavor _]
    :drop-table)
  (add-column [flavor _ _]
    :add-column)
  (drop-column [flavor _ _]
    :drop-column)
  (describe-table [flavor _]
    :describe-table)
  (delete [flavor _ _]
    :delete)
  (format-date [flavor _]
    :format-date)
  (format-date-time [flavor _]
    :format-date-time)
  (format-time [flavor _]
    :format-time))

(deftest test-protocol
  (let [test-flavor (TestFlavor.)]
    (is (= (db-map test-flavor) :db-map))
    (is (= (execute-query test-flavor nil) :execute-query))
    (is (= (execute-commands test-flavor nil) :execute-commands))
    (is (= (update test-flavor nil nil nil) :update))
    (is (= (insert-into test-flavor nil nil) :insert-into))
    (is (= (table-exists? test-flavor nil) :table-exists?))
    (is (= (sql-find test-flavor nil) :sql-find))
    (is (= (create-table test-flavor nil nil) :create-table))
    (is (= (drop-table test-flavor nil) :drop-table))
    (is (= (add-column test-flavor nil nil) :add-column))
    (is (= (drop-column test-flavor nil nil) :drop-column))
    (is (= (describe-table test-flavor nil) :describe-table))
    (is (= (delete test-flavor nil nil) :delete))
    (is (= (format-date test-flavor nil) :format-date))
    (is (= (format-date-time test-flavor nil) :format-date-time))
    (is (= (format-time test-flavor nil) :format-time))))