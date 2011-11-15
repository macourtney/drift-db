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

(defn-
#^{ :doc "Cleans up the given row, loading any clobs into memory." }
  clean-row [row]
  (reduce 
    (fn [new-map pair] 
        (assoc new-map (first pair) (clean-value (second pair))))
    {} 
    row))

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
#^{:doc "Returns the given key or string as valid column name. Basically turns 
any keyword into a string, and replaces dashes with underscores."}
  column-name-key [column-name]
  (when column-name
    (keyword (conjure-loading-utils/underscores-to-dashes (.toLowerCase column-name)))))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  auto-increment-mod [column-spec]
  (if (:auto-increment column-spec) ["AUTO_INCREMENT"] []))

(defn
#^{:doc "Returns the not null spec vector from the given mods map."}
  not-null-mod [column-spec]
  (if (:not-null column-spec) ["NOT NULL"] []))

(defn
#^{:doc "Returns the primary key spec vector from the given mods map."}
  primary-key-mod [column-spec]
  (if (:primary-key column-spec) ["PRIMARY KEY"] []))

(defmulti column-spec-vec (fn [column-spec] (:type column-spec)))

(defmethod column-spec-vec :date [column-spec]
  [(column-name (:name column-spec)) "DATE"])

(defmethod column-spec-vec :date-time [column-spec]
  [(column-name (:name column-spec)) "DATETIME"])

(defmethod column-spec-vec :integer [column-spec]
  (concat [(column-name (:name column-spec)) "INT"] (not-null-mod column-spec) (auto-increment-mod column-spec)
    (primary-key-mod column-spec)))

(defmethod column-spec-vec :id [column-spec]
  (column-spec-vec (assoc column-spec :type :integer)))

(defmethod column-spec-vec :belongs-to [column-spec]
  (column-spec-vec (assoc column-spec :type :integer)))

(defmethod column-spec-vec :decimal [column-spec]
  (let [precision (get column-spec :precision 20)
        scale (get column-spec :scale 6)
        decimal (str "DECIMAL(" precision "," scale ")")]
    (concat [(column-name (:name column-spec)) decimal] (not-null-mod column-spec) (primary-key-mod column-spec))))

(defmethod column-spec-vec :string [column-spec]
  (let [length (get column-spec :length 255)
        varchar (str "VARCHAR(" length ")")]
    (concat [(column-name (:name column-spec)) varchar] (not-null-mod column-spec) (primary-key-mod column-spec))))

(defmethod column-spec-vec :text [column-spec]
  [(column-name (:name column-spec)) "TEXT"])

(defmethod column-spec-vec :time [column-spec]
  [(column-name (:name column-spec)) "TIME"])

(defmulti spec-vec (fn [spec] (:spec-type spec)))

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
  (if (= (:key column-desc) "PRI")
    (assoc column-map :primary-key true)
    column-map))

(defn add-not-null [column-desc column-map]
  (if (= (:null column-desc) "NO")
    (assoc column-map :not-null true)
    column-map))

(defn add-length [column-desc column-map]
  (if-let [length (parse-length (:type column-desc))]
    (assoc column-map :length length)
    column-map))

(defn add-default [column-desc column-map]
  (if-let [default (:default column-desc)]
    (assoc column-map :default default)
    column-map))

(defn parse-precision [column-type]
  (when column-type
    (let [matcher (re-matcher decimal-regex column-type)]
      (when (.matches matcher)
        (Integer/parseInt (second (re-groups matcher)))))))

(defn add-precision [column-desc column-map]
  (if-let [precision (parse-precision (:type column-desc))]
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
  (if-let [scale (parse-scale (:type column-desc))]
    (assoc column-map :scale scale)
    column-map))

(defn parse-column [column-desc]
  ;(logging/info (str "column-desc: " column-desc))
  (add-scale column-desc
    (add-precision column-desc
      (add-default column-desc
        (add-length column-desc
          (add-not-null column-desc
            (add-primary-key column-desc
              { :name (column-name-key (:field column-desc))
                :type (parse-type (:type column-desc)) })))))))

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

  (update [flavor table where-params record]
    (do
      (logging/debug (str "Update table: " table " where: " where-params " record: " record))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/update-values (table-name table) where-params record))))

  (insert-into [flavor table records]
    (do
      (logging/debug (str "insert into: " table " records: " records))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/insert-records (table-name table) records))))

  (table-exists? [flavor table]
    (try
      (let [results (drift-db-protocol/execute-query flavor [(str "SELECT * FROM " (table-name table) " LIMIT 1")])]
        true)
      (catch Exception e false)))

  (sql-find [flavor select-map]
    (let [table (:table select-map)
          select-clause (or (:select select-map) "*")
          where-clause (:where select-map)
          limit-clause (:limit select-map)]
      (drift-db-protocol/execute-query flavor 
        [(str "SELECT " select-clause " FROM " (table-name table)
             (when where-clause (str " WHERE " where-clause)) 
             (when limit-clause (str " LIMIT " limit-clause)))])))

  (create-table [flavor table specs]
    (do
      (logging/debug (str "Create table: " table " with specs: " specs))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (apply sql/create-table (table-name table) (map spec-vec specs)))))

  (drop-table [flavor table]
    (do
      (logging/debug (str "Drop table: " (table-name table)))
      (when (some #(= (.toUpperCase (table-name table)) %) (map :table_name (drift-db-protocol/execute-query flavor ["SHOW TABLES"])))
        (sql/with-connection (drift-db-protocol/db-map flavor)
          (sql/drop-table (table-name table))))))

  (add-column [flavor table spec]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " ADD IF NOT EXISTS " (spec-str spec))]))

  (drop-column [flavor table column]
    (drift-db-protocol/execute-commands flavor
      [(str "ALTER TABLE " (table-name table) " DROP COLUMN IF EXISTS " (column-name column))]))

  (describe-table [flavor table]
    (do
      (logging/debug (str "Describe table: " table))
      { :name table
        :columns (map parse-column (drift-db-protocol/execute-query flavor [(str "SHOW COLUMNS FROM " (table-name table))])) }))

  (delete [flavor table where]
    (do
      (logging/debug (str "Delete from " table " where " where))
      (sql/with-connection (drift-db-protocol/db-map flavor)
        (sql/delete-rows (table-name table) where))))

  (format-date [flavor date]
    (. (new SimpleDateFormat "yyyy-MM-dd") format date))

  (format-date-time [flavor date]
    (. (new SimpleDateFormat "yyyy-MM-dd HH:mm:ss") format date))

  (format-time [flavor date]
    (. (new SimpleDateFormat "HH:mm:ss") format date)))

(defn h2-flavor
  ([dbname] (h2-flavor dbname nil))
  ([dbname db-dir]
    (H2Flavor. dbname db-dir)))