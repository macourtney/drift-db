(defproject org.drift-db/drift-db-mysql "1.1.1"
  :description "This is the mysql implementation of the drift-db protocol."
  :dependencies [[clojure-tools "1.1.1"]
                 [mysql/mysql-connector-java "5.1.21"]
                 [org.clojure/clojure "1.2.1"]
                 [org.clojure/java.jdbc "0.2.3"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.drift-db/drift-db "1.1.1"]])