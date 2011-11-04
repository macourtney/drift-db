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