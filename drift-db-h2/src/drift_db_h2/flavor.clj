(ns drift-db-h2.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.tools.string-utils :as conjure-string-utils]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol]
            [drift-db-h2.column :as h2-column])
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

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (h2-column/column-name-key (first pair)) (clean-value (second pair))))
    {} 
    row))

(defn convert-record [record]
  (reduce #(assoc %1 (h2-column/column-name (first %2)) (second %2)) {} record))

(defn convert-records [records]
  (map convert-record records))

(defn pair-to-equals [pair]
  (str "(" (h2-column/column-name (first pair)) " = ?)"))

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
  (str (h2-column/column-name (get clause :expression))
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
    (keyword? clause) (h2-column/column-name clause)
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
        (apply sql/create-table (table-name table) (map h2-column/spec-vec specs)))))

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
        :columns (map h2-column/parse-column
                      (drift-db-protocol/execute-query flavor [(str "SHOW COLUMNS FROM " (table-name table))])) }))

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD IF NOT EXISTS " (h2-column/spec-str spec))]))

  (drop-column [flavor table column]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN IF EXISTS " (h2-column/column-name column))]))
  
  (update-column [flavor table column spec]
    (when-let [old-column-name (h2-column/column-name column)]
      (let [column-name (or (h2-column/spec-column-name spec) old-column-name)]
        (when (not (= old-column-name column-name))
          (drift-db-protocol/execute-commands flavor
            [(str "ALTER TABLE " (table-name table) " ALTER COLUMN " old-column-name " RENAME TO " column-name)]))
        (drift-db-protocol/execute-commands flavor
          [(str "ALTER TABLE " (table-name table) " ALTER COLUMN " (h2-column/spec-str spec))]))))

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