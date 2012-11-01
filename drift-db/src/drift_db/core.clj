(ns drift-db.core
  (:refer-clojure :exclude [boolean])
  (:require [clojure.tools.logging :as logging]
            [clojure.tools.loading-utils :as loading-utils]
            [clojure.tools.string-utils :as string-utils]
            [drift-db.column.belongs-to :as belongs-to-column]
            [drift-db.column.boolean :as boolean-column]
            [drift-db.column.column :as column-protocol]
            [drift-db.column.date :as date-column]
            [drift-db.column.date-time :as date-time-column]
            [drift-db.column.decimal :as decimal-column]
            [drift-db.column.identifier :as identifier-column]
            [drift-db.column.integer :as integer-column]
            [drift-db.column.string :as string-column]
            [drift-db.column.text :as text-column]
            [drift-db.column.time :as time-column]
            [drift-db.protocol :as flavor-protocol]))

(def drift-db-flavor (atom nil))

(defn init-flavor
  "Sets the flavor used by Drift-db."
  [flavor]
  (swap! drift-db-flavor (fn [_] flavor)))

(defn initialized?
  "Returns true if and only if Drift-db has been initialized with a flavor."
  []
  (not (nil? @drift-db-flavor)))

(defn db-map
  "Returns the db-map in the loaded db-flavor."
  []
  (flavor-protocol/db-map @drift-db-flavor))

(defn execute-query
  "Runs the given sql query vector using the currently set protocol. The sql-vector is in the form [sql & params],
  where sql is the sql statment string and params is a list of values which will be passed to the sql statement string
  if it is a prepared statement. This function returns the results as a list of maps."
  [sql-vector]
  (flavor-protocol/execute-query @drift-db-flavor sql-vector))

(defn execute-commands
  "Runs the given set of sql strings and does not return results."
  [& sql-strings]
  (flavor-protocol/execute-commands @drift-db-flavor sql-strings))

(defn sql-find
  "Runs an sql select statement built from the given select-map. The valid keys are:

        table - the table to run the select statement on
        select - the columns to return. Either a string, or a vector of column names.
        where - the conditions as a string, vector, or prototype record
        limit - the number of rows to return
        offset - the starting row"
  [select-map]
  (flavor-protocol/sql-find @drift-db-flavor select-map))

;; Table Functions

(defn create-table
  "Creates a table in the database with the name table, and the given specs. Each spec is a map describing some part of
  the table. Right now, only column specs are supported."
  [table & specs]
  (flavor-protocol/create-table @drift-db-flavor table specs))

(defn drop-table
  "Drops the table with the given name from the database."
  [table]
  (flavor-protocol/drop-table @drift-db-flavor table))

(defn table-exists?
  "Returns true if the table with the given name exists."
  [table]
  (flavor-protocol/table-exists? @drift-db-flavor table))

(defn drop-table-if-exists
  "Drops the table with the given name from the database."
  [table]
  (when (table-exists? table)
    (drop-table table)))

(defn describe-table
  "Shows the columns of the given table. The result is a map which looks like:

       { :name <table name>
         :columns <columns specs> }
    
    Each column spec is exactly like the column spec passed into create table."
  [table]
  (flavor-protocol/describe-table @drift-db-flavor table))

;; Column Functions

(defn table-name
  "Returns the name of the table from the given table metadata"
  [table-metadata]
  (:name table-metadata))

(defn column-type
  "Returns the type of the column from the given column metadata"
  [column-metadata]
  (:type column-metadata))

(defn column-length
  "Returns the length of the column from the given column metadata"
  [column-metadata]
  (:length column-metadata))

(defn column-auto-increment
  "Returns the auto increment value of the column from the given column metadata"
  [column-metadata]
  (:auto-increment column-metadata))

(defn column-default
  "Returns the default value if it exists of the column from the given column metadata"
  [column-metadata]
  (:default column-metadata))

(defn column-not-null
  "Returns true if the column with the given metadata is not nullable."
  [column-metadata]
  (:not-null column-metadata))

(defn column-primary-key
  "Returns true if the column with the given metadata is the primary key."
  [column-metadata]
  (:primary-key column-metadata))

(defn add-column
  "Adds a column described by spec to the given table. Spec is a map describing a column."
  [table spec]
  (flavor-protocol/add-column @drift-db-flavor table spec))

(defn drop-column
  "Removes the given column from the given table."
  [table column]
  (flavor-protocol/drop-column @drift-db-flavor table column))

(defn update-column
  "Updates the given column to the given spec. Spec is exactly the same map used in add-column."
  [table column spec]
  (flavor-protocol/update-column @drift-db-flavor table column spec))

(defn column-name
  "Given a column name or column spec, this function returns the column name."
  [column]
  (keyword
    (cond
      (map? column) (get column :name)
      (keyword? column) (name column)
      (string? column) column
      (satisfies? column-protocol/Column column) (column-protocol/name column)
      :else (throw (RuntimeException. (str "Don't know how to get the name for a column of type: " (type column)))))))

(defn column-name=
  "Returns true if both of the given columns specs or names have equal column names."
  [column1 column2]
  (= (column-name column1) (column-name column2)))

(defn columns
  "Returns the list of columns of the given table. Table can be either the name of the table, or the full table map."
  [table]
  (when table
    (if (map? table)
      (get table :columns)
      (recur (describe-table table)))))

(defn find-column
  "Returns the given column from the give table. Column can be either the column name or a column spec. Table can be
  either the table name or the full table map."
  [table column]
  (some #(when (column-name= column %1) %1) (columns table)))

(defn column-exists?
  "Returns true if the given column exists in the given table. This function only compares the column name. Table can be
  either the name of the table or the table map. Column can be either the full column or just the column name. This
  function is an alias for find-column."
  [table column]
  (find-column table column))

(defn drop-column-if-exists
  "Drops the given column from the given table if and only if the column exists in the table."
  [table column]
  (when (column-exists? table column)
    (drop-column table column)))

;; Spec Functions

(defn date
  "Returns a new spec describing a date with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (date column {}))
  ([column mods]
    (date-column/create-column column)))

(defn date-time
  "Returns a new spec describing a date time with the given column and spec mods map. Use this method with the
   create-table method.

   Curently supported values for mods: None"
  ([column] (date-time column {}))
  ([column mods]
    (date-time-column/create-column column)))

(defn integer
  "Returns a new spec describing an integer with the given column and spec mods map. Use this method with the 
   create-table method.

   Curently supported values for mods:
       :length - The number of possible digits for this integer.
       :not-null - If the value of this key resolves to true, then add this column will be forced to be not null.
       :primary-key - If true, then make this column the primary key.
       :auto-increment - If true, then when no value is given, this integer will be automatically set to the next
                         highest integer of all values already in the table."
  ([column]
    (integer column {}))
  ([column mods]
    (integer-column/create-column column mods)))

(defn id
  "Returns a new spec describing the id for a table. Use this method with the create-table method."
  ([] (id :id))
  ([column]
    (identifier-column/create-column column)))

(defn belongs-to
  "Returns a new spec describing a text with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods is exactly the same as integer."
  ([model] (belongs-to model {}))
  ([model mods]
    (belongs-to-column/create-column model mods)))

(defn decimal
  "Returns a new spec describing a decimal with the given column and spec mods map. Use this method with the 
   create-table method.

   Curently supported values for mods:
       :not-null - If the value of this key resolves to true, then add this column will be forced to be not null.
       :primary-key - If true, then make this column the primary key.
       :precision - The number of digits of precision.
       :scale - The scale."
  ([column] (decimal column {}))
  ([column mods]
    (decimal-column/create-column column mods)))

(defn string
  "Returns a new spec describing a string with the given column and spec mods map. Use this method with the
   create-table method.

   Curently supported values for mods:
       :length - The length of the varchar, if not present then the varchar defaults to 255.
       :not-null - If the value of this key resolves to true, then add this column will be forced to be not null.
       :primary-key - If true, then make this column the primary key."
  ([column] (string column {}))
  ([column mods]
    (string-column/create-column column mods)))

(defn text
  "Returns a new spec describing a text with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (text column {}))
  ([column mods]
    (text-column/create-column column)))

(defn time-type
  "Returns a new spec describing a time with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (time-type column {}))
  ([column mods]
    (time-column/create-column column)))

(defn boolean
  "Returns a new spec describing a boolean with the given column and spec mods map. Use this method with the
   create-table method.

   Curently supported values for mods: None"
  ([column] (boolean column {}))
  ([column mods]
    (boolean-column/create-column column)))

(defn format-date
  "Returns the string value of the given date for use in the database."
  [date]
  (flavor-protocol/format-date @drift-db-flavor date))

(defn format-date-time
  "Returns the string value of the given date as a date time for use in the database."
  [date]
  (flavor-protocol/format-date-time @drift-db-flavor date))

(defn format-time
  "Returns the string value of the given date as a time for use in the database."
  [date]
  (flavor-protocol/format-time @drift-db-flavor date))

;; Row Functions

(defn insert-into
  "Inserts the given records into the given table.

      table - The name of the table to update.
      records - A map from strings or keywords (identifying columns) to updated values."
  [table & records]
  (flavor-protocol/insert-into @drift-db-flavor table records))

(defn delete
  "Deletes rows from the table which satisfies the given where or prototype record."
  [table where-or-record]
  (flavor-protocol/delete @drift-db-flavor table where-or-record))

(defn update
  "Runs an update given the table, where-params and a record.

      table - The name of the table to update.
      where-or-record - The where clause or a prototype record.
      record - A map from strings or keywords (identifying columns) to updated values."
  [table where-or-record record]
  (flavor-protocol/update @drift-db-flavor table where-or-record record))

(defn create-index
  "Creates an index on the given table using the given columns specified with the given spec. Supported keys in
     mods is:

       columns - The columns to use in the index.
       unique? - If true, then the index should be unique. Optional
       method - The name of the method to use. Optional, uses the database's default if missing. Supported values: btree, hash
       direction - The direction of the index order. Either ascending or descending. Optional.
       nulls - Where the nulls should be in the index order. Either first or last. Optional."
  [table index-name mods]
  (flavor-protocol/create-index @drift-db-flavor table index-name mods))

(defn drop-index
  "Drops the given index."
  [table index-name]
  (flavor-protocol/drop-index @drift-db-flavor table index-name))
