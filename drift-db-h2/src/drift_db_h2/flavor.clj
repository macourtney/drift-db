(ns drift-db-h2.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.tools.string-utils :as conjure-string-utils]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol])
  (:import
    [java.text SimpleDateFormat]
    [org.h2.jdbcx JdbcDataSource]
    [org.h2.jdbc JdbcClob]))

(defn
#^{:doc "Returns an h2 datasource for a ."}
  create-datasource
    ([connection-url] (create-datasource connection-url nil nil))
    ([connection-url username password]
      (let [h2-datasource (new JdbcDataSource)]
        (. h2-datasource setURL connection-url)
        (when (and username password)
          (. h2-datasource setUser username)
          (. h2-datasource setPassword password))
        h2-datasource)))

(defn-
#^{ :doc "Cleans up the given value, loading any clobs into memory." }
  clean-value [value]
  (if (and value (instance? JdbcClob value))
    (.getSubString value 1 (.length value))
    value))

(defn
#^{:doc "Returns the given key or string as valid table name. Basically turns any keyword into a string, and replaces
dashes with underscores."}
  table-name [table]
  (conjure-loading-utils/dashes-to-underscores (conjure-string-utils/str-keyword table)))

(defn
#^{:doc "Returns the given key or string as valid column name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  column-name [column]
  (conjure-loading-utils/dashes-to-underscores (conjure-string-utils/str-keyword column)))

(defn
#^{ :doc "Returns the given valid column name as a keyword. Basically turns 
any string into a keyword, and replaces underscores with dashes." }
  column-name-key [column-name]
  (when column-name
    (keyword (conjure-loading-utils/underscores-to-dashes (.toLowerCase (name column-name))))))

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (column-name-key (first pair)) (clean-value (second pair))))
    {} 
    row))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  auto-increment-mod [column-spec]
  (if (get column-spec :auto-increment) ["AUTO_INCREMENT"] []))

(defn
#^{:doc "Returns the not null spec vector from the given mods map."}
  not-null-mod [column-spec]
  (if (get column-spec :not-null) ["NOT NULL"] []))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  primary-key-mod [column-spec]
  (if (get column-spec :primary-key) ["PRIMARY KEY"] []))

(defn spec-column-name
  "Returns the column name from the given columns spec"
  [column-spec]
  (column-name (get column-spec :name)))

(defmulti column-spec-vec (fn [column-spec] (get column-spec :type)))

(defmethod column-spec-vec :date [column-spec]
  [(spec-column-name column-spec) "DATE"])

(defmethod column-spec-vec :date-time [column-spec]
  [(spec-column-name column-spec) "DATETIME"])

(defmethod column-spec-vec :integer [column-spec]
  (concat [(spec-column-name column-spec) (if (> (get column-spec :length 9) 9) "BIGINT" "INT")]
          (not-null-mod column-spec) (auto-increment-mod column-spec) (primary-key-mod column-spec)))

(defmethod column-spec-vec :id [column-spec]
  (column-spec-vec (merge column-spec { :type :integer :auto-increment true })))

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

(def date-regex #"DATE\((\d+)\)")
(def date-time-regex #"TIMESTAMP\((\d+)\)")
(def integer-regex #"INTEGER\((\d+)\)")
(def decimal-regex #"DECIMAL\((\d+)(,\s*(\d+))?\)")
(def varchar-regex #"VARCHAR\((\d+)\)")
(def text-regex #"CLOB\((\d+)\)")
(def time-regex #"TIME\((\d+)\)")

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

(defn parse-date-length [column-type]
  (parse-length-with-regex column-type date-regex))

(defn parse-date-time-length [column-type]
  (parse-length-with-regex column-type date-time-regex))

(defn parse-integer-length [column-type]
  (parse-length-with-regex column-type integer-regex))

(defn parse-string-length [column-type]
  (parse-length-with-regex column-type varchar-regex))

(defn parse-text-length [column-type]
  (parse-length-with-regex column-type text-regex))

(defn parse-time-length [column-type]
  (parse-length-with-regex column-type time-regex))

(defn parse-length [column-type]
  (when column-type
    (cond
      (is-date-column column-type) (parse-date-length column-type)
      (is-date-time-column column-type) (parse-date-time-length column-type)
      (is-integer-column column-type) (parse-integer-length column-type)
      (is-string-column column-type) (parse-string-length column-type)
      (is-text-column column-type) (parse-text-length column-type)
      (is-time-column column-type) (parse-time-length column-type))))

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
    (if (= default "NULL")
      column-map
      (assoc column-map :default default))
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

(defn parse-column [column-desc]
  (add-auto-increment column-desc
    (add-scale column-desc
      (add-precision column-desc
        (add-default column-desc
          (add-length column-desc
            (add-not-null column-desc
              (add-primary-key column-desc
                { :name (column-name-key (get column-desc :field))
                  :type (parse-type (get column-desc :type)) }))))))))

(defn convert-record [record]
  (reduce #(assoc %1 (column-name (first %2)) (second %2)) {} record))

(defn convert-records [records]
  (map convert-record records))

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

(defn select-clause
  "Returns the columns to return for a select statement."
  [select-map]
  (convert-select (get select-map :select)))

(defn where-clause
  "Returns where clause for a select statement."
  [select-map]
  (when-let [clause (convert-where (get select-map :where))]
    (str " WHERE " (first clause))))

(defn where-values
  "Returns where values for a select statement."
  [select-map]
  (when-let [clause (convert-where (get select-map :where))]
    (rest clause)))

(defn limit-clause
  "Returns the limit clause for a select statement."
  [select-map]
  (when-let [clause (get select-map :limit)]
    (str " LIMIT " clause)))

(defn offset-clause
  "Returns the offset clause for a select statement."
  [select-map]
  (when-let [clause (get select-map :offset)]
    (str " OFFSET " clause)))

(defn map-order-clause [clause]
  (str (column-name (get clause :expression))
    (when-let [direction (get clause :direction)]
      (let [direction (clojure-str/lower-case (name direction))]
        (if (or (= direction "ascending") (= direction "asc"))
          " ASC"
          " DESC")))
    (when-let [nulls (get clause :nulls)]
      (str " NULLS " (if (= (clojure-str/lower-case (name nulls)) "first") "FIRST" "LAST")))))

(defn order-str [clause]
  (cond
    (map? clause) (map-order-clause clause)
    (or (vector? clause) (seq? clause)) (clojure-str/join ", " (map order-str clause))
    (keyword? clause) (column-name clause)
    :else clause))

(defn order-clause
  "Returns the offset clause for a select statement."
  [select-map]
  (when-let [clause (get select-map :order-by)]
    (str " ORDER BY " (order-str clause))))

(deftype H2Flavor [dbname db-dir]
  drift-db-protocol/Flavor
  (db-map [flavor]
    (let [subprotocol "h2"
          db-directory (or db-dir "db/data/")
          subname (str db-directory dbname)]
      { :flavor flavor

        ;; The name of the JDBC driver to use.
        :classname "org.h2.Driver"

        ;; The database type.
        :subprotocol subprotocol

        ;; The database path.
        :subname subname

        ;; A datasource for the database.
        :datasource (create-datasource (format "jdbc:%s:%s" subprotocol subname)) }))

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
    (let [select-str (str "SELECT " (select-clause select-map) " FROM " (table-name (get select-map :table))
                       (where-clause select-map)
                       (order-clause select-map)
                       (limit-clause select-map)
                       (offset-clause select-map))]
      (drift-db-protocol/execute-query flavor
        (vec (cons select-str (where-values select-map))))))

  (create-table [flavor table specs]
    (do
      (logging/debug (str "Create table: " table " with specs: " specs))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/create-table (table-name table) (map spec-vec specs)))))

  (drop-table [flavor table]
    (do
      (logging/debug (str "Drop table: " (table-name table)))
      (when (some #(= (.toUpperCase (table-name table)) %)
              (map #(get % :table-name) (drift-db-protocol/execute-query flavor ["SHOW TABLES"])))
        (sql/with-connection (drift-db-protocol/db-map flavor)
          (sql/drop-table (table-name table))))))

  (table-exists? [flavor table]
    (try
      (let [results (drift-db-protocol/execute-query flavor [(str "SELECT * FROM " (table-name table) " LIMIT 1")])]
        true)
      (catch Exception e false)))

  (describe-table [flavor table]
    (do
      (logging/debug (str "Describe table: " table))
      { :name table
        :columns (map parse-column (drift-db-protocol/execute-query flavor [(str "SHOW COLUMNS FROM " (table-name table))])) }))

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD IF NOT EXISTS " (spec-str spec))]))

  (drop-column [flavor table column]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN IF EXISTS " (column-name column))]))
  
  (update-column [flavor table column spec]
    (when-let [old-column-name (column-name column)]
      (let [column-name (or (spec-column-name spec) old-column-name)]
        (when (not (= old-column-name column-name))
          (drift-db-protocol/execute-commands flavor
            [(str "ALTER TABLE " (table-name table) " ALTER COLUMN " old-column-name " RENAME TO " column-name)]))
        (drift-db-protocol/execute-commands flavor
          [(str "ALTER TABLE " (table-name table) " ALTER COLUMN " (spec-str (assoc spec :name column-name)))]))))

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
        (apply sql/insert-records (table-name table) (convert-records records)))))

  (delete [flavor table where-or-record]
    (do
      (logging/debug (str "Delete from " table " where " where-or-record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/delete-rows (table-name table) (convert-where where-or-record)))))

  (update [flavor table where-or-record record]
    (do
      (logging/debug (str "Update table: " table " where: " where-or-record " record: " record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/update-values (table-name table) (convert-where where-or-record) (convert-record record))))))

(defn h2-flavor
  ([dbname] (h2-flavor dbname nil))
  ([dbname db-dir]
    (H2Flavor. dbname db-dir)))