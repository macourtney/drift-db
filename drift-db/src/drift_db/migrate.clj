(ns drift-db.migrate
  (:require [clojure.tools.logging :as logging]
            [drift-db.core :as core]))

(def schema-info-table "schema_info")
(def version-column :version)

(defn
  version-table-is-empty []
  (logging/info (str schema-info-table " is empty. Setting the initial version to 0."))
  (core/insert-into schema-info-table { version-column 0 })
  0)

(defn
  version-table-exists []
  (logging/info (str schema-info-table " exists"))
  (if-let [version-result-map (first (core/sql-find { :table schema-info-table :limit 1 }))]
    (get version-result-map version-column)
    (version-table-is-empty)))

(defn
  version-table-does-not-exist []
  (logging/info (str schema-info-table " does not exist. Creating table..."))
  (core/create-table schema-info-table 
    (core/integer version-column { :length 19 :not-null true }))
  (version-table-is-empty))

(defn 
#^{:doc "Gets the current db version number. If the schema info table doesn't exist this function creates it. If the 
schema info table is empty, then it adds a row and sets the version to 0."}
  current-version []
  (if (core/table-exists? schema-info-table)
    (version-table-exists)
    (version-table-does-not-exist)))

(defn
#^{:doc "Updates the version number saved in the schema table in the database."}
  update-version [new-version]
  (core/update schema-info-table ["true"] { version-column new-version }))