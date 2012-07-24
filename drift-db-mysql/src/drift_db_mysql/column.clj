(ns drift-db-mysql.column
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
#^{:doc "Returns the given string surrounded by backquotes."}
  backquote [s]
  (str "`" s "`"))

(defn
#^{:doc "Returns the given key or string as valid column name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  column-name [column]
  (backquote (conjure-loading-utils/dashes-to-underscores (name column))))

(defn
#^{ :doc "Returns the given valid column name as a keyword. Basically turns 
any string into a keyword, and replaces underscores with dashes." }
  column-name-key [column-name]
  (when column-name
    (keyword (conjure-loading-utils/underscores-to-dashes (.toLowerCase (name column-name))))))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  auto-increment-mod [column-spec]
  (if (column-protocol/auto-increment? column-spec)
    ["AUTO_INCREMENT"]
    []))

(defn
#^{:doc "Returns the not null spec vector from the given mods map."}
  not-null-mod [column-spec]
  (if (column-protocol/nullable? column-spec)
    []
    ["NOT NULL"]))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  primary-key-mod [column-spec]
  (if (column-protocol/primary-key? column-spec)
    ["PRIMARY KEY"]
    []))

(defn spec-column-name
  "Returns the column name from the given columns spec"
  [column-spec]
  (column-name (column-protocol/name column-spec)))

(defn integer-db-type [column-spec]
  (let [length (or (column-protocol/length column-spec) 11)]
    (str
      (cond
        (< length 3) "TINYINT"
        (> length 11) "BIGINT"
        :else "INT")
      "(" length ")")))

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
      "DATETIME")

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
  (concat [(db-type column-spec)] (not-null-mod column-spec) (auto-increment-mod column-spec)
          (primary-key-mod column-spec))) 

(defn column-spec-vec [column-spec]
  (cons (spec-column-name column-spec) (type-vec column-spec)))

(defmulti spec-vec (fn [spec] (spec-protocol/type spec)))

(defmethod spec-vec :column [spec]
  (column-spec-vec spec))

(defn spec-str [spec]
  (clojure-str/join " " (spec-vec spec)))

(def date-regex #"date")
(def date-time-regex #"datetime")
(def integer-regex #"(int|tinyint)\((\d+)\)")
(def decimal-regex #"decimal\((\d+)(,\s*(\d+))?\)")
(def varchar-regex #"varchar\((\d+)\)")
(def text-regex #"text")
(def time-regex #"time")

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
  (parse-length-with-regex column-type integer-regex 2))

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
  (if (= (get column-desc :null) "NO")
    (assoc column-map :not-null true)
    column-map))

(defn add-length [column-desc column-map]
  (if-let [length (parse-length (get column-desc :type))]
    (assoc column-map :length length)
    column-map))

(defn add-default [column-desc column-map]
  (if-let [default (get column-desc :default)]
    (assoc column-map :default default)
    column-map))

(defn parse-precision [column-type]
  (when column-type
    (let [matcher (re-matcher decimal-regex column-type)]
      (when (.matches matcher)
        (Integer/parseInt (second (re-groups matcher)))))))

(defn add-precision [column-desc column-map]
  (if-let [precision (parse-precision (get column-desc :type))]
    (assoc column-map :precision precision)
    column-map))

(defn parse-scale [column-type]
  (when column-type
    (let [matcher (re-matcher decimal-regex column-type)]
      (when (and (.matches matcher) (> (.groupCount matcher) 2))
        (let [scale-str (.group matcher 3)]
          (when (not-empty scale-str)
            (Integer/parseInt scale-str)))))))

(defn add-scale [column-desc column-map]
  (if-let [scale (parse-scale (get column-desc :type))]
    (assoc column-map :scale scale)
    column-map))

(defn add-auto-increment [column-desc column-map]
  (if-let [default (get column-desc :default)]
    (if (.startsWith default "(NEXT VALUE FOR PUBLIC")
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
                  { :name (column-name-key (get column-desc :field))
                    :type (parse-type (get column-desc :type)) })))))))))