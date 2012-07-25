(ns drift-db-mysql.flavor
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.loading-utils :as conjure-loading-utils]
            [clojure.tools.logging :as logging]
            [clojure.string :as clojure-str]
            [drift-db.protocol :as drift-db-protocol]
            [drift-db-mysql.column :as column])
  (:import [com.mysql.jdbc.jdbc2.optional MysqlDataSource]
           [java.text SimpleDateFormat]))

(defn
#^{:doc "Returns an mysql datasource for a ."}
  create-datasource
    ([connection-url] (create-datasource connection-url nil nil))
    ([connection-url username password]
      (let [mysql-datasource (new MysqlDataSource)]
        (.setURL mysql-datasource connection-url)
        (when (and username password)
          (.setUser mysql-datasource username)
          (.setPassword mysql-datasource password))
        mysql-datasource)))

(defn
#^{:doc "Returns the given key or string as valid table name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  table-name [table]
  (column/backquote (conjure-loading-utils/dashes-to-underscores (name table))))

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (column/column-name-key (first pair)) (second pair)))
    {} 
    row))

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
  (str (column/column-name (get clause :expression))
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
    (keyword? clause) (column/column-name clause)
    :else clause))

(defn order-clause
  "Returns the offset clause for a select statement."
  [select-map]
  (when-let [clause (get select-map :order-by)]
    (str " ORDER BY " (order-str clause))))

(deftype MysqlFlavor [username password dbname host]
  drift-db-protocol/Flavor
  (db-map [flavor]
    (let [subprotocol "mysql"
          subname (str "//" host "/" dbname)]

      { :flavor flavor

        ;; The name of the JDBC driver to use.
        :classname "com.mysql.jdbc.Driver"

        ;; A datasource for the database.
        :datasource (create-datasource (format "jdbc:%s:%s" subprotocol subname))
        
        ;; The user name to use when connecting to the database.
        :username username

        ;; The password to use when connecting to the database.
        :password password }))

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
        :columns (map column/parse-column (drift-db-protocol/execute-query flavor [(str "SHOW COLUMNS FROM " (table-name table))]))}))

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD " (column/spec-str spec))]))
  
  (drop-column [flavor table column-spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN " (column/column-name column-spec))]))

  (update-column [flavor table column spec]
    (when-let [old-column-name (column/column-name column)]
      (let [column-name (or (column/spec-column-name spec) old-column-name)]
        (if (not (= old-column-name column-name))
          (drift-db-protocol/execute-commands flavor
            [(str "ALTER TABLE " (table-name table) " CHANGE COLUMN " old-column-name " " (column/spec-str spec))])
          (drift-db-protocol/execute-commands flavor
            [(str "ALTER TABLE " (table-name table) " MODIFY COLUMN " (column/spec-str (assoc spec :name column-name)))])))))

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
        (sql/update-values (table-name table) (convert-where where-or-record) record))))

  (create-index [flavor table index-name mods]
    (logging/debug (str "Adding index: " index-name " to table: " table " with mods: " mods))
    (drift-db-protocol/execute-commands flavor
      [(str "CREATE " (when (:unique? mods) "UNIQUE ") "INDEX " (column/db-symbol index-name)
            (when (= (clojure-str/lower-case (name (:method mods))) "hash") " USING HASH") " ON "
            (table-name table) "(" (clojure-str/join "," (map column/column-name (:columns mods))) ")")]))

  (drop-index [flavor table index-name]
    (logging/debug (str "Dropping index: " index-name " on table: " table))
    (drift-db-protocol/execute-commands flavor
      [(str "DROP INDEX " (column/db-symbol index-name) " ON " (table-name table))])))

(defn mysql-flavor
  ([username password dbname] (mysql-flavor username password dbname "localhost"))
  ([username password dbname host]
    (MysqlFlavor. username password dbname host)))