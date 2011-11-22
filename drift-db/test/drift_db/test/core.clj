(ns drift-db.test.core
  (:use [drift-db.core])
  (:use [clojure.test]))

(defn assert-column-spec
  ([column-spec column-type column-name] (assert-column-spec column-spec column-type column-name {}))
  ([column-spec column-type column-name mods]
    (is (= column-spec
          (merge
            { :spec-type :column
              :type column-type
              :name column-name }
            mods)))))

(defn assert-date-spec [date-spec column-name]
  (assert-column-spec date-spec :date column-name))

(deftest test-date
  (assert-date-spec (date :test) :test)
  (assert-date-spec (date :test { :fail :fail }) :test))

(defn assert-date-time-spec [date-spec column-name]
  (assert-column-spec date-spec :date-time column-name))

(deftest test-date-time
  (assert-date-time-spec (date-time :test) :test)
  (assert-date-time-spec (date-time :test { :fail :fail }) :test))

(defn update-map 
  ([map key value]
    (if (and map key value)
      (assoc map key value)
      map))
  ([map key value & others]
    (apply update-map (update-map map key value) others)))

(defn assert-integer-spec
  ([date-spec column-name]
    (assert-column-spec date-spec :integer column-name))
  ([date-spec column-name not-null primary-key]
    (assert-column-spec date-spec :integer column-name (update-map {} :not-null not-null :primary-key primary-key))))

(deftest test-integer
  (assert-integer-spec (integer :test) :test)
  (assert-integer-spec (integer :test { :not-null true }) :test true nil)
  (assert-integer-spec (integer :test { :primary-key true }) :test nil true)
  (assert-integer-spec (integer :test { :not-null true :primary-key true }) :test true true)
  (assert-integer-spec (integer :test { :fail :fail }) :test))

(deftest test-column-name
  (is (= (column-name :foo) :foo))
  (is (= (column-name "foo") :foo))
  (is (= (column-name { :name :foo }) :foo))
  (is (= (column-name { :name "foo" }) :foo)))

(deftest test-column-name=
  (is (column-name= :foo :foo))
  (is (not (column-name= :foo :bar)))
  (is (column-name= :foo "foo"))
  (is (not (column-name= :foo "bar")))
  (is (column-name= :foo { :name :foo }))
  (is (not (column-name= :foo { :name :bar })))
  (is (column-name= :foo { :name "foo" }))
  (is (not (column-name= :foo { :name "bar" }))))

(deftest test-columns
  (let [test-columns [{ :default "" :length 20 :not-null true :primary-key true :name :name :type :string }
                      { :name :created-at :type :date }
                      { :name :edited-at :type :date-time }]]
    (is (= (columns { :name :test :columns test-columns }) test-columns))))

(deftest test-find-column
  (let [test-column-name :created-at
        test-column { :name test-column-name :type :date }
        test-columns [{ :default "" :length 20 :not-null true :primary-key true :name :name :type :string }
                      test-column
                      { :name :edited-at :type :date-time }]
        test-table { :name :test :columns test-columns }]
    (is (= (find-column test-table test-column-name) test-column))
    (is (= (find-column test-table test-column) test-column))))