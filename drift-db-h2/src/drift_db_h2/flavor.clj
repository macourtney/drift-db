(ns drift-db-h2.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.tools.string-utils :as conjure-string-utils]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol]
            [drift-db-h2.column :as h2-column])
  (:import
    [java.sql Clob]
    [java.text SimpleDateFormat]
    [org.h2.jdbcx JdbcDataSource]
    [org.h2.jdbc JdbcClob]))

(defn clob-string [clob]
  (when clob
    (let [clob-stream (.getCharacterStream clob)]
      (try
        (clojure-str/join
          "\n" (take-while identity (repeatedly #(.readLine clob-stream))))
        (catch Exception e
          (logging/error (str "An error occured while reading a clob: " e)))))))

(defn- clean-value
  "Cleans up the given value, loading any clobs into memory."
  [value]
  (cond
    (instance? Clob value) (clob-string value)
    (instance? JdbcClob value) (clob-string value)
    :else value))

(defn table-name
  "Returns the given key or string as valid table name. Basically turns any keyword into a string, and replaces
dashes with underscores."
  [table]
  (conjure-loading-utils/dashes-to-underscores (conjure-string-utils/str-keyword table)))

(defn- clean-row
  "Cleans up the given row, loading any clobs into memory."
  [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (h2-column/column-name-key (first pair))
               (clean-value (second pair))))
    {} 
    row))

(defn convert-record [record]
  (reduce #(assoc %1 (h2-column/column-name (first %2)) (second %2)) {} record))

(defn convert-records [records]
  (map convert-record (filter identity records)))

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

(deftype H2Flavor [dbname db-dir username password encrypt? file-password]
  drift-db-protocol/Flavor
  (db-map [flavor]
    (let [subprotocol "h2"
          db-directory (or db-dir "db/data/")
          subname (str db-directory dbname (when encrypt? ";CIPHER=AES"))
          password (if encrypt? (str password " " (or file-password password)) password)]
      { :flavor flavor

        ;; The name of the JDBC driver to use.
        :classname "org.h2.Driver"

        ;; The database type.
        :subprotocol subprotocol

        ;; The database path.
        :subname subname
        
        ;; The database username
        :username username
        
        ;; The combined file password and database password
        :password password }))

  (execute-query [flavor sql-vector]
    (do
      (logging/debug (str "Executing query: " sql-vector))
      (sql/query (drift-db-protocol/db-map flavor) sql-vector
          :row-fn clean-row)))

  (execute-commands [flavor sql-strings]
    (logging/debug (str "Executing update: " (seq sql-strings)))
    (apply sql/db-do-commands (drift-db-protocol/db-map flavor) sql-strings))

  (sql-find [flavor select-map]
    (let [select-str (str "SELECT " (select-clause select-map) " FROM " 
                       (table-name (get select-map :table))
                       (where-clause select-map)
                       (order-clause select-map)
                       (limit-clause select-map)
                       (offset-clause select-map))]
      (drift-db-protocol/execute-query flavor
        (vec (cons select-str (where-values select-map))))))

  (create-table [flavor table specs]
    (logging/debug (str "Create table: " table " with specs: " specs))
    (drift-db-protocol/execute-commands flavor
      [(apply sql/create-table-ddl (table-name table)
              (map h2-column/spec-vec specs))]))

  (drop-table [flavor table]
    (logging/debug (str "Drop table: " (table-name table)))
    (let [clean-table-name (table-name table)
          h2-table-name (clojure-str/upper-case clean-table-name)]
      (when (drift-db-protocol/table-exists? flavor clean-table-name)
        (drift-db-protocol/execute-commands
          flavor
          [(sql/drop-table-ddl clean-table-name)]))))

  (table-exists? [flavor table]
    (try
      (let [results (drift-db-protocol/execute-query
                      flavor
                      [(str "SELECT * FROM " (table-name table) " LIMIT 1")])]
        true)
      (catch Exception e false)))

  (describe-table [flavor table]
    (do
      (logging/debug (str "Describe table: " table))
      { :name (table-name table)
        :columns (map h2-column/parse-column
                      (drift-db-protocol/execute-query
                        flavor
                        [(str "SHOW COLUMNS FROM " (table-name table))])) }))

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
    (logging/debug (str "insert into: " table " records: " records))
    (apply sql/insert! (drift-db-protocol/db-map flavor) (table-name table)
           (convert-records records)))

  (delete [flavor table where-or-record]
    (logging/debug (str "Delete from " table " where " where-or-record))
    (sql/delete! (drift-db-protocol/db-map flavor) (table-name table)
                 (convert-where where-or-record)))

  (update [flavor table where-or-record record]
    (logging/debug "Update table:" table "where:" where-or-record "record:"
                   record)
    (sql/update! (drift-db-protocol/db-map flavor) (table-name table)
                 (convert-record record) (convert-where where-or-record)))

  (create-index [flavor table index-name mods]
    (logging/debug (str "Adding index: " index-name " to table: " table " with mods: " mods))
    (drift-db-protocol/execute-commands flavor
      [(str "CREATE " (when (:unique? mods) "UNIQUE ") "INDEX IF NOT EXISTS "
            (h2-column/db-symbol index-name) " ON " (table-name table) "("
            (clojure-str/join "," (map h2-column/column-name (:columns mods)))
            ")")]))

  (drop-index [flavor table index-name]
    (logging/debug (str "Dropping index: " index-name " on table: " table))
    (drift-db-protocol/execute-commands flavor
      [(str "DROP INDEX IF EXISTS " (h2-column/db-symbol index-name))]))

  (table-column-name [flavor column] (h2-column/column-name column)))

(defn h2-flavor
  ([dbname] (h2-flavor dbname nil))
  ([dbname db-dir] (h2-flavor dbname db-dir nil nil))
  ([dbname db-dir username password] (h2-flavor dbname db-dir username password false))
  ([dbname db-dir username password encrypt?] (h2-flavor dbname db-dir username password encrypt? nil))
  ([dbname db-dir username password encrypt? file-password]
    (H2Flavor. dbname db-dir username password encrypt? file-password)))