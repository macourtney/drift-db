(ns drift-db-postgresql.column
  (:require [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.string-utils :as conjure-string-utils]
            [clojure.string :as clojure-str]
            [drift-db.column.column :as column-protocol]
            [drift-db.column.belongs-to :as belongs-to-column]
            [drift-db.column.date :as date-column]
            [drift-db.column.date-time :as date-time-column]
            [drift-db.column.decimal :as decimal-column]
            [drift-db.column.identifier :as identifier-column]
            [drift-db.column.integer :as integer-column]
            [drift-db.column.string :as string-column]
            [drift-db.column.text :as text-column]
            [drift-db.column.time :as time-column]
            [drift-db.core :as drift-db]
            [drift-db.spec :as spec-protocol])
  (:import [drift_db.column.belongs_to BelongsToColumn]
           [drift_db.column.boolean BooleanColumn]
           [drift_db.column.date DateColumn]
           [drift_db.column.date_time DateTimeColumn]
           [drift_db.column.decimal DecimalColumn]
           [drift_db.column.identifier IdentifierColumn]
           [drift_db.column.integer IntegerColumn]
           [drift_db.column.string StringColumn]
           [drift_db.column.text TextColumn]
           [drift_db.column.time TimeColumn]))

(defn
#^{:doc "Returns the given string surrounded by double quotes."}
  identifier-quote [s]
  (str "\"" s "\""))

(defn db-symbol
  "Converts the given symbol-name which can be a string or keyword, and converts it to a proper database symbol."
  [symbol-name]
  (identifier-quote (conjure-loading-utils/dashes-to-underscores (name symbol-name))))

(defn
#^{:doc "Returns the given key or string as valid column name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  column-name [column]
  (if (satisfies? column-protocol/Column column)
    (column-name (column-protocol/name column))
    (db-symbol column)))

(defn
#^{ :doc "Returns the given valid column name as a keyword. Basically turns 
any string into a keyword, and replaces underscores with dashes." }
  column-name-key [column-name]
  (when column-name
    (keyword (conjure-loading-utils/underscores-to-dashes (.toLowerCase (name column-name))))))

(defn nullable?
  "Returns true if the given columns spec is nullable which means not-null is false or not set or auto-increment is
   true."
  [column-spec]
  (or (column-protocol/nullable? column-spec) (column-protocol/auto-increment? column-spec)))

(defn
#^{:doc "Returns the not null spec vector from the given mods map."}
  not-null-mod [column-spec]
  (if (nullable? column-spec)
    []
    ["NOT NULL"]))

(defn primary-key?
  "Returns true if the given column-spec should be a primary key which means primary-key is true and auto-increment is
   false."
  [column-spec]
  (and (column-protocol/primary-key? column-spec) (not (column-protocol/auto-increment? column-spec))))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  primary-key-mod [column-spec]
  (if (primary-key? column-spec)
    ["PRIMARY KEY"]
    []))

(defn spec-column-name
  "Returns the column name from the given columns spec"
  [column-spec]
  (column-name (column-protocol/name column-spec)))

(defn integer-db-type [column-spec]
  (if (column-protocol/auto-increment? column-spec)
    "SERIAL"
    (let [int-length (or (column-protocol/length column-spec) 9)]
      (cond (< int-length 5) "SMALLINT" (> int-length 9) "BIGINT" :else "INTEGER"))))

(defprotocol DBType
  (db-type [column-spec] "Returns the type for the given column spec."))

(extend-protocol DBType
  BelongsToColumn
    (db-type [column-spec]
      (integer-db-type column-spec))

  BooleanColumn
    (db-type [column-spec]
      "BOOLEAN")

  DateColumn
    (db-type [column-spec]
      "DATE")

  DateTimeColumn
    (db-type [column-spec]
      "TIMESTAMP")

  DecimalColumn
    (db-type [column-spec]
      (str "DECIMAL(" (or (column-protocol/precision column-spec) 20) "," (or (column-protocol/scale column-spec) 6) ")"))

  IntegerColumn
    (db-type [column-spec]
      (integer-db-type column-spec))

  IdentifierColumn
    (db-type [column-spec]
      (integer-db-type column-spec))

  StringColumn
    (db-type [column-spec]
      (str "VARCHAR(" (or (column-protocol/length column-spec) 255) ")"))

  TextColumn
    (db-type [column-spec]
      "TEXT")

  TimeColumn
    (db-type [column-spec]
      "TIME"))

(defn type-vec [column-spec]
  (concat [(db-type column-spec)] (not-null-mod column-spec) (primary-key-mod column-spec))) 

(defn column-spec-vec [column-spec]
  (cons (spec-column-name column-spec) (type-vec column-spec)))

(defmulti spec-vec (fn [spec] (spec-protocol/type spec)))

(defmethod spec-vec :column [spec]
  (column-spec-vec spec))

(defn spec-str [spec]
  (clojure-str/join " " (spec-vec spec)))

(def boolean-regex #"boolean")
(def date-regex #"date")
(def date-time-regex #"timestamp without time zone")
(def integer-regex #"smallint|integer|bigint")
(def decimal-regex #"numeric")
(def varchar-regex #"character varying")
(def text-regex #"text")
(def time-regex #"time without time zone")

(defn is-boolean-column [column-type]
  (re-matches boolean-regex column-type))

(defn is-date-column [column-type]
  (re-matches date-regex column-type))

(defn is-date-time-column [column-type]
  (re-matches date-time-regex column-type))

(defn is-integer-column [column-type]
  (re-matches integer-regex column-type))

(defn is-decimal-column [column-type]
  (re-matches decimal-regex column-type))

(defn is-string-column [column-type]
  (re-matches varchar-regex column-type))

(defn is-text-column [column-type]
  (re-matches text-regex column-type))

(defn is-time-column [column-type]
  (re-matches time-regex column-type))

(defn parse-type [column-type]
  (when column-type
    (cond
      (is-boolean-column column-type) :boolean
      (is-date-column column-type) :date
      (is-date-time-column column-type) :date-time
      (is-integer-column column-type) :integer
      (is-decimal-column column-type) :decimal
      (is-string-column column-type) :string
      (is-text-column column-type) :text
      (is-time-column column-type) :time
      :else (throw (RuntimeException. (str "Unknown column type: " column-type))))))

(defn parse-length-with-regex
  ([column-type regex] (parse-length-with-regex column-type regex 1))
  ([column-type regex length-group]
    (let [matcher (re-matcher regex column-type)]
      (when (.matches matcher)
        (Integer/parseInt (nth (re-groups matcher) length-group))))))


(defn parse-integer-length [column-type]
  (parse-length-with-regex column-type integer-regex))

(defn parse-string-length [column-type]
  (parse-length-with-regex column-type varchar-regex))

(defn parse-length [column-type]
  (when column-type
    (cond
      (is-integer-column column-type) (parse-integer-length column-type)
      (is-string-column column-type) (parse-string-length column-type))))

(defn add-primary-key [column-desc column-map]
  (if (= (get column-desc :key) "PRI")
    (assoc column-map :primary-key true)
    column-map))

(defn add-not-null [column-desc column-map]
  (if (= (get column-desc :is-nullable) "NO")
    (assoc column-map :not-null true)
    column-map))

(defn add-length [column-desc column-map]
  (if-let [length (:character-maximum-length column-desc)]
    (assoc column-map :length length)
    column-map))

(defn add-default [column-desc column-map]
  (if-let [default (get column-desc :default)]
    (assoc column-map :default default)
    column-map))

(defn add-precision [column-desc column-map]
  (if (= (:type column-map) :decimal)
    (if-let [precision (:numeric-precision column-desc)]
      (assoc column-map :precision precision)
      column-map)
    column-map))

(defn add-scale [column-desc column-map]
  (if (= (:type column-map) :decimal)
    (if-let [scale (:numeric-scale column-desc)]
      (assoc column-map :scale scale)
      column-map)
    column-map))

(defn add-auto-increment [column-desc column-map]
  (if-let [extra (get column-desc :column-default)]
    (if (.startsWith extra "nextval(")
      (assoc column-map :auto-increment true)
      column-map)
    column-map))

(defmulti create-column  (fn [column-map] (get column-map :type)))

(defmethod create-column :date [column-map]
  (drift-db/date (get column-map :name)))

(defmethod create-column :date-time [column-map]
  (drift-db/date-time (get column-map :name)))

(defmethod create-column :integer [column-map]
  (drift-db/integer (get column-map :name) column-map))

(defmethod create-column :decimal [column-map]
  (drift-db/decimal (get column-map :name) column-map))

(defmethod create-column :string [column-map]
  (drift-db/string (get column-map :name) column-map))

(defmethod create-column :text [column-map]
  (drift-db/text (get column-map :name)))

(defmethod create-column :time [column-map]
  (drift-db/time-type (get column-map :name)))

(defmethod create-column :boolean [column-map]
  (drift-db/boolean (get column-map :name)))

(defmethod create-column :default [column-map]
  (throw (RuntimeException. (str "Cannot create a column for: " column-map))))

(defn parse-column [column-desc] 
  (create-column
    (add-auto-increment column-desc
      (add-scale column-desc
        (add-precision column-desc
          (add-default column-desc
            (add-length column-desc
              (add-not-null column-desc
                (add-primary-key column-desc
                  { :name (column-name-key (get column-desc :column-name))
                    :type (parse-type (get column-desc :data-type)) })))))))))