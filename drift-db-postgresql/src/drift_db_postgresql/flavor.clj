(ns drift-db-postgresql.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol]
            [drift-db-postgresql.column :as column])
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
#^{:doc "Returns the given key or string as valid table name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  table-name [table]
  (column/identifier-quote (conjure-loading-utils/dashes-to-underscores (name table))))

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (column/column-name-key (first pair)) (second pair)))
    {} 
    row))

(defn clean-record-for-db
  "Cleans the given record, preparing it for insertion into the db. Calls column/db-symbol on each of the keys in the
given record."
  [record]
    (reduce
      #(assoc %1 (column/db-symbol (first %2)) (second %2))
      {} record))

(defn clean-all-records-for-db
  "Calls clean-record-for-db for each record in the given sequence of records and returns the result in a new sequence.
If the record is nil, it is removed from the result sequence."
  [records]
  (map clean-record-for-db (filter identity records)))

(defn pair-to-equals [pair]
  (str "(" (column/column-name (first pair)) " = ?)"))

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
      (clojure-str/join ", " (map #(if (keyword? %1) (column/column-name %1) (name %1)) select-or-list)))
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

(defn lower-case [named]
  (when named
    (clojure-str/lower-case (name named))))

(defn nulls [mods]
  (when-let [nulls (get mods :nulls)]
    (str " NULLS " (if (= (lower-case nulls) "first") "FIRST" "LAST"))))

(defn direction [mods]
  (when-let [direction (get mods :direction)]
    (let [direction (lower-case direction)]
      (if (or (= direction "ascending") (= direction "asc"))
        " ASC"
        " DESC"))))

(defn map-order-clause [clause]
  (str (column/column-name (get clause :expression))
    (direction clause)
    (nulls clause)))

(defn order-str [clause]
  (cond
    (map? clause) (map-order-clause clause)
    (or (vector? clause) (seq? clause)) (clojure-str/join ", " (map order-str clause))
    (keyword? clause) (column/column-name clause)
    :else clause))

(defn order-clause
  "Returns the offset clause for a select statement."
  [select-map]
  (when-let [clause (get select-map :order-by)]
    (str " ORDER BY " (order-str clause))))

(defn index-column [column mods]
  (str (column/column-name column)
       (direction mods)
       (nulls mods)))

(defn index-columns [mods]
  (clojure-str/join "," (map #(index-column % mods) (:columns mods))))

(defn index-method [mods]
  (when-let [method (lower-case (:method mods))]
    (str " USING "
         (condp = method
           "btree" "BTREE"
           "hash" "HASH"
           "gist" "GIST"
           "gin" "GIN"
           (throw (RuntimeException. (str "Unknown method: " method)))))))

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
        (apply sql/create-table (table-name table) (map column/spec-vec specs)))))

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
        :columns (map column/parse-column 
                      (drift-db-protocol/execute-query flavor 
                        [(str "SELECT column_name, data_type, character_maximum_length, numeric_scale, numeric_precision, is_nullable, column_default FROM information_schema.columns WHERE table_name = '" (conjure-loading-utils/dashes-to-underscores (name table)) "';")]))})) ; 

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD " (column/spec-str spec))]))
  
  (drop-column [flavor table column-spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN " (column/column-name column-spec))]))

  (update-column [flavor table column-name spec]
    (when-let [old-column-name (column/column-name column-name)]
      (let [column-name (or (column/spec-column-name spec) old-column-name)]
        (when (not (= old-column-name column-name))
          (drift-db-protocol/execute-commands flavor
            [(str "ALTER TABLE " (table-name table) " RENAME " old-column-name " TO " column-name)]))
        (drift-db-protocol/execute-commands flavor
          [(str "ALTER TABLE " (table-name table) " ALTER COLUMN " column-name " TYPE " (column/db-type spec))
           (if (column/nullable? spec)
             (str "ALTER TABLE " (table-name table) " ALTER COLUMN " column-name " DROP NOT NULL")
             (str "ALTER TABLE " (table-name table) " ALTER COLUMN " column-name " SET NOT NULL"))]))))

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
        (apply sql/insert-records (table-name table) (clean-all-records-for-db records)))))

  (delete [flavor table where-or-record]
    (do
      (logging/debug (str "Delete from " table " where " where-or-record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/delete-rows (table-name table) (convert-where where-or-record)))))

  (update [flavor table where-or-record record]
    (do
      (logging/debug (str "Update table: " table " where: " where-or-record " record: " record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/update-values (table-name table) (convert-where where-or-record) (clean-record-for-db record)))))

  (create-index [flavor table index-name mods]
    (logging/debug (str "Adding index: " index-name " to table: " table " with mods: " mods))
    (drift-db-protocol/execute-commands flavor
      [(str "CREATE " (when (:unique? mods) "UNIQUE ") "INDEX " (column/db-symbol index-name)
            " ON " (table-name table) (index-method mods)
            " (" (index-columns mods) ")")]))

  (drop-index [flavor table index-name]
    (logging/debug (str "Dropping index: " index-name " on table: " table))
    (drift-db-protocol/execute-commands flavor
      [(str "DROP INDEX IF EXISTS " (column/db-symbol index-name))]))

  (table-column-name [flavor column] (column/column-name column)))

(defn postgresql-flavor
  ([username password dbname] (postgresql-flavor username password dbname "localhost"))
  ([username password dbname host]
    (PostgresqlFlavor. username password dbname host)))