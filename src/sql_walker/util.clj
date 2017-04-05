(ns sql-walker.util)

;; todo: cleanup
(require '[clojure.core.reducers :as r]
         '[clojure.string :as s]
         '[clojure.java.jdbc :as jdbc]
         '[clojure.reflect :as reflect]
         '[clojure.data.xml :as x]
         '[clojure.data.generators :as gen]
         '[selmer.parser :as parser]
         '[selmer.util :refer [without-escaping]]

         '[camel-snake-kebab.core :as c]
         '[gen-mybatis.core.sql.generators :as sqlgen ])
(defn get-fields
  "Gets the field names from a type"
  [t]
  (filter (fn [x] (instance? clojure.reflect.Field x)) (:members (reflect/type-reflect t))))
