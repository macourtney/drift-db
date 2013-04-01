(ns drift-db.test.core
  (:refer-clojure :exclude [boolean byte-array])
  (:use [drift-db.core])
  (:use [clojure.test])
  (:require [drift-db.column.column :as column]
            [drift-db.spec :as spec]))

(defn assert-spec-type [column-spec]
  (is (= (spec/type column-spec) :column)))

(defn assert-column-type [column-spec column-type]
  (is (= (column/type column-spec) column-type)))

(defn assert-column-name [column-spec column-name]
  (is (= (column/name column-spec) column-name)))

(defn assert-nullable? [column-spec nullable?]
  (is (= (column/nullable? column-spec) nullable?)))

(defn assert-primary-key? [column-spec primary-key?]
  (if primary-key?
    (is (column/primary-key? column-spec))
    (is (not (column/primary-key? column-spec)))))

(defn assert-auto-increment? [column-spec auto-increment?]
  (is (= (column/auto-increment? column-spec) auto-increment?)))

(defn assert-column-spec [column-spec column-type column-name]
  (assert-spec-type column-spec)
  (assert-column-type column-spec column-type)
  (assert-column-name column-spec column-name))

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

(defn assert-integer-spec
  ([integer-spec column-name]
    (assert-column-spec integer-spec :integer column-name))
  ([integer-spec column-name not-null primary-key auto-increment]
    (assert-column-spec integer-spec :integer column-name)
    (assert-nullable? integer-spec (not not-null))
    (assert-primary-key? integer-spec primary-key)
    (assert-auto-increment? integer-spec auto-increment)))

(deftest test-integer
  (assert-integer-spec (integer :test) :test)
  (assert-integer-spec (integer :test { :not-null true }) :test true nil false)
  (assert-integer-spec (integer :test { :primary-key true }) :test nil true false)
  (assert-integer-spec (integer :test { :not-null true :primary-key true }) :test true true false)
  (assert-integer-spec (integer :test { :auto-increment true }) :test false false true)
  (assert-integer-spec (integer :test { :fail :fail }) :test))

(deftest test-column-name
  (is (= (column-name :foo) :foo))
  (is (= (column-name "foo") :foo))
  (is (= (column-name { :name :foo }) :foo))
  (is (= (column-name { :name "foo" }) :foo))
  (is (= (column-name (integer :foo)) :foo)))

(deftest test-column-name=
  (is (column-name= :foo :foo))
  (is (not (column-name= :foo :bar)))
  (is (column-name= :foo "foo"))
  (is (not (column-name= :foo "bar")))
  (is (column-name= :foo { :name :foo }))
  (is (not (column-name= :foo { :name :bar })))
  (is (column-name= :foo { :name "foo" }))
  (is (not (column-name= :foo { :name "bar" })))
  (is (column-name= (integer :foo) :foo))
  (is (not (column-name= (integer :foo) :bar)))
  (is (column-name= (integer :foo) (integer :foo)))
  (is (not (column-name= (integer :foo) (integer :bar)))))

(deftest test-columns
  (let [test-columns [{ :default "" :length 20 :not-null true :primary-key true :name :name :type :string }
                      { :name :created-at :type :date }
                      { :name :edited-at :type :date-time }
                      (integer :foo)]]
    (is (= (columns { :name :test :columns test-columns }) test-columns))
    (is (nil? (columns nil)))))

(deftest test-find-column
  (let [test-column-name :created-at
        test-column { :name test-column-name :type :date }
        test-column2 (integer :foo)
        test-columns [{ :default "" :length 20 :not-null true :primary-key true :name :name :type :string }
                      test-column
                      { :name :edited-at :type :date-time }
                      test-column2]
        test-table { :name :test :columns test-columns }]
    (is (= (find-column test-table test-column-name) test-column))
    (is (= (find-column test-table test-column) test-column))
    (is (= (find-column test-table :foo) test-column2))))

(def test-column-metadata-1 { :auto-increment true, :default "(NEXT VALUE FOR PUBLIC.SYSTEM_SEQUENCE_D36D2960_AD38_447B_A6F4_4F202EE4E709)", :length 10, :not-null true, :primary-key true, :name :id, :type :integer })
(def test-column-metadata-2 { :length 255, :name :text, :type :string })

(def test-table-metadata { :name "messages", :columns (test-column-metadata-1 test-column-metadata-2) })

(deftest test-table-name
  (is (= (:name test-table-metadata) (table-name test-table-metadata)))
  (is (nil? (table-name nil))))

(deftest test-column-type
  (is (= (:type test-column-metadata-1) (column-type test-column-metadata-1)))
  (is (= (:type test-column-metadata-2) (column-type test-column-metadata-2)))
  (is (nil? (column-type nil))))

(deftest test-column-length
  (is (= (:length test-column-metadata-1) (column-length test-column-metadata-1)))
  (is (= (:length test-column-metadata-2) (column-length test-column-metadata-2)))
  (is (nil? (column-length nil))))

(deftest test-column-auto-increment
  (is (= (:auto-increment test-column-metadata-1) (column-auto-increment test-column-metadata-1)))
  (is (= (:auto-increment test-column-metadata-2) (column-auto-increment test-column-metadata-2)))
  (is (nil? (column-auto-increment nil))))

(deftest test-column-default
  (is (= (:default test-column-metadata-1) (column-default test-column-metadata-1)))
  (is (= (:default test-column-metadata-2) (column-default test-column-metadata-2)))
  (is (nil? (column-default nil))))

(deftest test-column-not-null
  (is (= (:not-null test-column-metadata-1) (column-not-null test-column-metadata-1)))
  (is (= (:not-null test-column-metadata-2) (column-not-null test-column-metadata-2)))
  (is (nil? (column-not-null nil))))

(deftest test-column-primary-key
  (is (= (:primary-key test-column-metadata-1) (column-primary-key test-column-metadata-1)))
  (is (= (:primary-key test-column-metadata-2) (column-primary-key test-column-metadata-2)))
  (is (nil? (column-primary-key nil))))

(deftest test-belongs-to
  (assert-column-spec (belongs-to "test") :belongs-to "test_id")
  (assert-column-spec (belongs-to :test-id) :belongs-to "test_id")
  (assert-column-spec (belongs-to :test-id) :belongs-to "test_id"))