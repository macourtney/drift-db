(ns drift-db-postgresql.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol])
  (:import [org.postgresql.ds PGSimpleDataSource]
           [java.text SimpleDateFormat]))

(defn
#^{:doc "Returns an mysql datasource for a ."}
  create-datasource
  ([server-name database-name] (create-datasource server-name database-name nil nil))
  ([server-name database-name username password]
    (let [mysql-datasource (doto (new PGSimpleDataSource)
                             (.setServerName server-name)
                             (.setDatabaseName database-name))]
      (if (and username password)
        (doto mysql-datasource
          (.setUser username)
          (.setPassword password))
        mysql-datasource))))

(defn
#^{:doc "Returns the given string surrounded by double quotes."}
  identifier-quote [s]
  (str "\"" s "\""))

(defn
#^{:doc "Returns the given key or string as valid table name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  table-name [table]
  (identifier-quote (conjure-loading-utils/dashes-to-underscores (name table))))

(defn
#^{:doc "Returns the not null spec vector from the given mods map."}
  not-null-mod [mods]
  (if (get mods :not-null) ["NOT NULL"] []))
  
(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  primary-key-mod [mods]
  (if (get mods :primary-key) ["PRIMARY KEY"] []))
  
(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  auto-increment-mod [mods]
  (if (get mods :auto-increment) ["AUTO_INCREMENT"] []))

(defn
#^{:doc "Returns the given key or string as valid column name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  column-name [column]
  (identifier-quote (conjure-loading-utils/dashes-to-underscores (name column))))

(defn
#^{:doc "Returns the given valid column name as a keyword. Basically turns 
any string into a keyword, and replaces underscores with dashes."}
  column-name-key [column-name]
  (when column-name
    (keyword (conjure-loading-utils/underscores-to-dashes (.toLowerCase (name column-name))))))

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (column-name-key (first pair)) (second pair)))
    {} 
    row))

(defn spec-column-name
  "Returns the column name from the given columns spec"
  [column-spec]
  (column-name (get column-spec :name)))

(defmulti column-spec-vec (fn [column-spec] (get column-spec :type)))

(defmethod column-spec-vec :date [column-spec]
  [(spec-column-name column-spec) "DATE"])

(defmethod column-spec-vec :date-time [column-spec]
  [(spec-column-name column-spec) "TIMESTAMP"])

(defmethod column-spec-vec :integer [column-spec]
  
  (if (:auto-increment column-spec)
    [(spec-column-name column-spec) "SERIAL"]
    (let [int-length (get column-spec :length 11)
          int-type (cond (< int-length 5) "SMALLINT" (> int-length 9) "BIGINT" :else "INTEGER")
          integer int-type]
      (concat [(spec-column-name column-spec) integer] (not-null-mod column-spec) (primary-key-mod column-spec)))))

(defmethod column-spec-vec :id [column-spec]
  (column-spec-vec (assoc column-spec :type :integer)))

(defmethod column-spec-vec :belongs-to [column-spec]
  (column-spec-vec (assoc column-spec :type :integer)))

(defmethod column-spec-vec :decimal [column-spec]
  (let [precision (get column-spec :precision 20)
        scale (get column-spec :scale 6)
        decimal (str "DECIMAL(" precision "," scale ")")]
    (concat [(spec-column-name column-spec) decimal] (not-null-mod column-spec) (primary-key-mod column-spec))))

(defmethod column-spec-vec :string [column-spec]
  (let [length (get column-spec :length 255)
        varchar (str "VARCHAR(" length ")")]
    (concat [(spec-column-name column-spec) varchar] (not-null-mod column-spec) (primary-key-mod column-spec))))

(defmethod column-spec-vec :text [column-spec]
  [(spec-column-name column-spec) "TEXT"])

(defmethod column-spec-vec :time [column-spec]
  [(spec-column-name column-spec) "TIME"])

(defmulti spec-vec (fn [spec] (get spec :spec-type)))

(defmethod spec-vec :column [spec]
  (column-spec-vec spec))

(defn spec-str [spec]
  (clojure-str/join " " (spec-vec spec)))

(def date-regex #"date")
(def date-time-regex #"timestamp without time zone")
(def integer-regex #"smallint|integer|bigint")
(def decimal-regex #"numeric")
(def varchar-regex #"character varying")
(def text-regex #"text")
(def time-regex #"time without time zone")

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
      (is-time-column column-type) :time)))

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
  (if-let [precision (:numeric-precision column-desc)]
    (assoc column-map :precision precision)
    column-map))

(defn add-scale [column-desc column-map]
  (if-let [scale (:numeric-scale column-desc)]
    (assoc column-map :scale scale)
    column-map))

(defn add-auto-increment [column-desc column-map]
  (if-let [extra (get column-desc :column-default)]
    (if (.startsWith extra "nextval(")
      (assoc column-map :auto-increment true)
      column-map)
    column-map))

(defn parse-column [column-desc]
  ;(logging/info (str "column-desc: " column-desc))
  (add-auto-increment column-desc
    (add-scale column-desc
      (add-precision column-desc
        (add-default column-desc
          (add-length column-desc
            (add-not-null column-desc
              (add-primary-key column-desc
                { :name (column-name-key (get column-desc :column-name))
                  :type (parse-type (get column-desc :data-type)) }))))))))

(defn pair-to-equals [pair]
  (str "(" (column-name (first pair)) " = ?)"))

(defn record-to-and-call [record]
  (cons (clojure-str/join " and " (map pair-to-equals record)) (vals record)))

(defn convert-where [where-or-record]
  (if (map? where-or-record)
    (record-to-and-call where-or-record)
    where-or-record))

(defn convert-select [select-or-list]
  (if select-or-list
    (if (string? select-or-list)
      select-or-list
      (clojure-str/join ", " (map name select-or-list)))
    "*"))

(deftype PostgresqlFlavor [username password dbname host]
  drift-db-protocol/Flavor
  (db-map [flavor]
    { :flavor flavor

      ;; The name of the JDBC driver to use.
      :classname "org.postgresql.Driver"

      ;; A datasource for the database.
      :datasource (create-datasource host dbname username password)
      
      ;; The user name to use when connecting to the database.
      :username username

      ;; The password to use when connecting to the database.
      :password password })

  (execute-query [flavor sql-vector]
    (do
      (logging/debug (str "Executing query: " sql-vector))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/with-query-results rows sql-vector
          (doall (map clean-row rows))))))

  (execute-commands [flavor sql-strings]
    (do
      (logging/debug (str "Executing update: " (seq sql-strings)))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/do-commands sql-strings))))

  (sql-find [flavor select-map]
    (let [table (get select-map :table)
          select-clause (convert-select (get select-map :select))
          where-clause (convert-where (get select-map :where))
          limit-clause (get select-map :limit)
          offset-clause (get select-map :offset)
          select-str (str "SELECT " select-clause " FROM " (table-name table)
                       (when where-clause (str " WHERE " (first where-clause))) 
                       (when limit-clause (str " LIMIT " limit-clause))
                       (when offset-clause (str " OFFSET " offset-clause)))]
      (drift-db-protocol/execute-query flavor
        (vec (concat [select-str] (when where-clause (rest where-clause)))))))

  (create-table [flavor table specs]
    (do
      (logging/debug (str "Create table: " table " with specs: " specs))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/create-table (table-name table) (map spec-vec specs)))))

  (drop-table [flavor table]
    (do
      (logging/debug (str "Drop table: " table))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/drop-table (table-name table)))))

  (table-exists? [flavor table]
    (try
      (let [results (drift-db-protocol/execute-query flavor [(str "SELECT * FROM " (table-name table) " LIMIT 1")])]
        true)
      (catch Exception e false)))

  (describe-table [flavor table]
    (do
      (logging/debug (str "Describe table: " table))
      { :name table
        :columns (map parse-column 
                      (drift-db-protocol/execute-query flavor 
                        [(str "SELECT column_name, data_type, character_maximum_length, numeric_scale, numeric_precision, is_nullable, column_default FROM information_schema.columns WHERE table_name = '" (name table) "';")]))})) ; 

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD " (spec-str spec))]))
  
  (drop-column [flavor table column]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN " (column-name column))]))

  (format-date [flavor date]
    (. (new SimpleDateFormat "yyyy-MM-dd") format date))

  (format-date-time [flavor date]
    (. (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss") format date))

  (format-time [flavor date]
    (. (new SimpleDateFormat "HH:mm:ss") format date))

  (insert-into [flavor table records]
    (do
      (logging/debug (str "insert into: " table " records: " records))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/insert-records (table-name table) records))))

  (delete [flavor table where-or-record]
    (do
      (logging/debug (str "Delete from " table " where " where-or-record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/delete-rows (table-name table) (convert-where where-or-record)))))

  (update [flavor table where-or-record record]
    (do
      (logging/debug (str "Update table: " table " where: " where-or-record " record: " record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/update-values (table-name table) (convert-where where-or-record) record)))))

(defn postgresql-flavor
  ([username password dbname] (postgresql-flavor username password dbname "localhost"))
  ([username password dbname host]
    (PostgresqlFlavor. username password dbname host)))