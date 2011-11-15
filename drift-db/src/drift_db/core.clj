(ns drift-db.core
  (:require [drift-db.protocol :as flavor-protocol]))

(def conjure-flavor (atom nil))

(defn init-flavor [flavor]
  (swap! conjure-flavor (fn [_] flavor)))

(defn execute-query [sql-vector]
  (flavor-protocol/execute-query @conjure-flavor sql-vector))

(defn execute-commands [& sql-strings]
  (flavor-protocol/execute-commands @conjure-flavor sql-strings))

(defn create-table [table & specs]
  (flavor-protocol/create-table @conjure-flavor table specs))

(defn delete [table where]
  (flavor-protocol/delete @conjure-flavor table where))

(defn describe-table [table]
  (flavor-protocol/describe-table @conjure-flavor table))

(defn drop-table [table]
  (flavor-protocol/drop-table @conjure-flavor table))

(defn add-column [table spec]
  (flavor-protocol/add-column @conjure-flavor table spec))

(defn drop-column [table column]
  (flavor-protocol/drop-column @conjure-flavor table column))

(defn insert-into [table & records]
  (flavor-protocol/insert-into @conjure-flavor table records))

(defn sql-find [select-map]
  (flavor-protocol/sql-find @conjure-flavor select-map))

(defn table-exists? [table]
  (flavor-protocol/table-exists? @conjure-flavor table))

(defn update [table where-params record]
  (flavor-protocol/update @conjure-flavor table where-params record))

(defn date
  "Returns a new spec describing a date with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (date column {}))
  ([column mods]
    { :spec-type :column
      :type :date
      :name column }))

(defn date-time
  "Returns a new spec describing a date time with the given column and spec mods map. Use this method with the
   create-table method.

   Curently supported values for mods: None"
  ([column] (date-time column {}))
  ([column mods]
    { :spec-type :column
      :type :date-time
      :name column }))

(defn integer
  "Returns a new spec describing an integer with the given column and spec mods map. Use this method with the 
   create-table method.

   Curently supported values for mods:
       :not-null - If the value of this key resolves to true, then add this column will be forced to be not null.
       :primary-key - If true, then make this column the primary key."
  ([column]
    (integer column {}))
  ([column mods]
    (merge
      { :spec-type :column
        :type :integer
        :name column }
      (select-keys mods [:not-null :primary-key]))))

(defn id
  "Returns a new spec describing the id for a table. Use this method with the create-table method."
  ([] (id :id))
  ([column]
    (assoc (integer column { :not-null true :primary-key true }) :type :id)))

(defn belongs-to
  "Returns a new spec describing a text with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods is exactly the same as integer."
  ([model] (belongs-to model {}))
  ([model mods]
    (assoc (integer model mods) :type :belongs-to)))

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
    (merge
      { :spec-type :column
        :type :decimal
        :name column }
      (select-keys mods [:not-null :primary-key :precision :scale]))))

(defn string
  "Returns a new spec describing a string with the given column and spec mods map. Use this method with the
   create-table method.

   Curently supported values for mods:
       :length - The length of the varchar, if not present then the varchar defaults to 255.
       :not-null - If the value of this key resolves to true, then add this column will be forced to be not null.
       :primary-key - If true, then make this column the primary key."
  ([column] (string column {}))
  ([column mods]
    (merge
      { :spec-type :column
        :type :string
        :name column }
      (select-keys mods [:length :not-null :primary-key]))))

(defn text
  "Returns a new spec describing a text with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (text column {}))
  ([column mods]
    { :spec-type :column
      :type :text
      :name column }))

(defn time-type
  "Returns a new spec describing a time with the given column and spec mods map. Use this method with the create-table
   method.

   Curently supported values for mods: None"
  ([column] (time-type column {}))
  ([column mods]
    { :spec-type :column
      :type :time
      :name column }))

(defn format-date [date]
  (flavor-protocol/format-date @conjure-flavor date))

(defn format-date-time [date]
  (flavor-protocol/format-date-time @conjure-flavor date))

(defn format-time [date]
  (flavor-protocol/format-time @conjure-flavor date))