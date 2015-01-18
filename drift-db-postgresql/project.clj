(defproject org.drift-db/drift-db-postgresql "1.1.8-SNAPSHOT"
  :description "This is the postgresql implementation of the drift-db protocol."
  :dependencies [[clojure-tools "1.1.2"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.drift-db/drift-db "1.1.8-SNAPSHOT"]
                 [postgresql "9.1-901.jdbc4"]])
