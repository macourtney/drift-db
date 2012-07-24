(defproject org.drift-db/drift-db-postgresql "1.1.1"
  :description "This is the postgresql implementation of the drift-db protocol."
  :dependencies [[clojure-tools "1.1.1"]
                 [org.clojure/clojure "1.2.1"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.drift-db/drift-db "1.1.1"]
                 [postgresql "9.1-901.jdbc4"]])