(ns sql-walker.mybatis)

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

(def null-conditional-template
  (str
   "<if test=\"[{var}] != '' and [{var}] != null\">
    <bind name=\"[{var}]Pattern\" value=\"'%' + [{var}] + '%'\" />
    AND [{col}] like #{[{var}]Pattern}
   </if>"))

(defn make-extra-filter
  [h]
  (parser/render null-conditional-template
                {:col (:col h)
                 :var (:var h)}
                {:tag-open \[
                 :tag-close \]}))

(defn make-extra-filters
  [colvarmap]
  (reduce str
          (map (fn [h] (make-extra-filter h)) colvarmap)))

(defn make-ors
  "Make the OR clauses"
  [filter-cols var]

  (defn make-ors-inner
    [s col var]
    (if (or (empty? col) (empty? var))
      ""
      (str " OR " col " LIKE #{" var "}")))

  (let [s ""]
    (if (or (empty? filter-cols) (empty? var))
      s
      (str (make-ors-inner s (first filter-cols) var)
           (make-ors (rest filter-cols) var)))))


(def select-column-template
  (str "{% if colprefix|not-empty %}{{colprefix}}.{% endif %}{{col}}{% if colsuffix|not-empty %} as {{colsuffix}}_{{col}}{% endif %}"))

(def main-template
  (str
   "<select id=\"{{queryId}}\" resultMap=\"{{map}}\">
        SELECT {{cols}}
        FROM [{{schema}}].{{table}}
        <where>
          {{where-clause}}
          {{null-conditionals}}
        </where>
    </select>"))

(defn get-schemas
  [db]
  (jdbc/query db "select * from sys.schemas"))

(defn get-tables-sql
  [schema]
  (parser/render
   (str  "select * from sys.objects "
         " where type_desc = 'USER_TABLE' "
         " and schema_id = (select schema_id from sys.schemas where name = '{{schema}}')")
   {:schema schema}))

(defn query-get-tables
  [db schema]
  (jdbc/query db (get-tables-sql schema)))

(defn get-column-sql
  [schema table]
  (str "select c.name col_name, t.name type_name, c.max_length length, c.precision, c.scale, c.is_identity"
 " from sys.columns c"
 " inner join sys.types t on t.user_type_id = c.user_type_id"
 " where object_id = ("
 " select object_id"
 " from sys.objects"
 " where type_desc = 'USER_TABLE'"
 " and name = '"
 table
 "'and schema_id = (select schema_id from sys.schemas where name = '"
 schema
 "'))"))

(defn inner-join [schema table table-alias] (str "INNER JOIN [" schema "]." table " " table-alias " ON v." table "Id = " table-alias ".PrimaryId"))

(defn query-get-columns
  [db schema table]
  (jdbc/query db [(get-column-sql schema table)]))

(defn query-get-column-map
  [db schema table]
  (let [cols (query-get-columns db schema table)]
    (zipmap  (map keyword (map :col_name cols))
             (map (fn [x] nil) cols))))

(defn mybatis-include [table] (str "<include refid=\"" (s/lower-case table) "Columns\">"
              "<property name=\"tableAlias\" value=\"" (str (first (s/lower-case table))) "\" />"
              "<property name=\"columnAlias\"" " value=\"" table "\"/>
           </include>"))

(defn mybatis-bind
  ([parameter] (mybatis-bind parameter parameter))
  ([name parameter] (str "<bind name=\"" name "Pattern\" value=\"'%' + " parameter " + '%'\" />")))

(defn mybatis-select-columns
  "Generate columns for mybatis select"
  [db schema table prefix colprefix]
  (let [rows (query-get-columns db schema table)]
    (map (fn [row] (str prefix "." (:col_name row) " as " colprefix "_" (:col_name row))) rows)))

(defn mybatis-sql-statment
  [db schema table prefix colprefix])

(defn mybatis-column-sql
  [column alias prefix]
  (str alias "." column " as " prefix "_" column ",
"))

(defn mybatis-table-column-sql
  [db schema table prefix colprefix]
  (->> (query-get-columns db schema table)
       (map :col_name )
       (map (fn [name] (mybatis-column-sql name prefix colprefix)))))

(defn mybatis-type-alias
  [class]
  (let
      [type (subs (str class) 6)
       short (last (s/split type #"\."))]
    (str "<typeAlias alias=\"" short "\" type=\"" type "\" />")))


(defn mybatis-result-map [id] (str "<resultMap id=\"" id "Map\" type=\""id "\">"
        "<id property=\"id\" column=\"PrimaryId\" />"
        "<result property=\"name\" column=\"Name\" />"
        "</resultMap>"))

(defn mybatis-association [id] (str "<association property=\"" id "\" resultMap=\""id "Map\" columnPrefix=\"" id "_\"/>"))

(defn get-person-columns
  [db schema table]
  (mybatis-select-columns
   db schema table "v" "Vital"))

(defn get-entity-columns
  "Gets columns from a table schema"
  [db schema table]
  (mybatis-select-columns
   db schema table "em" "Entity"))

(defn get-addr-columns
  "Gets columns from a table schema"
  [db schema table]
  (mybatis-select-columns
   db schema table "addr" "Address"))

(defn get-phone-columns
  "Gets columns from a table schema"
  [db schema table]
  (mybatis-select-columns
   db schema table "p" "Phone"))

(def entities
  [{:name "Master"}
   {:name "Address"}
   {:name "Contact"}
   {:name "Member"}
   {:name "Phone"}
   {:name "Vital"}])

(def schemas
  [{:name "Audit", :schema_id 5}
   {:name "Cache", :schema_id 6}
   {:name "Case", :schema_id 7}
   {:name "Entity", :schema_id 8}
   {:name "Lookup", :schema_id 9}
   {:name "Security", :schema_id 10}
   {:name "Template", :schema_id 11}
   {:name "Transient", :schema_id 12}])

(defn get-insert-map
  [db schema table]
  (let [pks (get-pk db schema table)
        cols (->> (query-get-columns db schema table)
                  (filter (fn [col] (not (.contains (map :column_name pks)
                                                    (:col_name col))))))]    
    (zipmap  (map keyword (map :col_name cols))
             (map sqlgen/generate-value cols))))

(defn get-insert-statement
  [db schema table]
  (let [pks (get-pk db schema table)
        cols (->> (query-get-columns db schema table)
                  (filter (fn [col] (not (.contains (map :column_name pks)
                                                    (:col_name col))))))]
    (sqlgen/sql-insert-statement schema table
                                 (map sqlgen/generate-value
                                      cols))))

(defn get-update-statement-with-var
  [db schema table varname]
  (let [pks (get-pk db schema table)
        cols (->> (query-get-columns db schema table)
                  (filter (fn [col] (not (.contains (map :column_name pks)
                                                    (:col_name col))))))]
    (str
     "update [" schema "].[" table "]
set ("
     (s/join ",
" (map (fn [col] (str (:col_name col) "=#{" varname "}")) cols))
     ")")))

(defn get-insert-statement-with-var
  [db schema table varname]
  (let [pks (get-pk db schema table)
        cols (->> (query-get-columns db schema table)
                  (filter (fn [col] (not (.contains (map :column_name pks)
                                                    (:col_name col))))))]
    (str
     "insert into [" schema "].[" table "]
("
     (s/join ",
" (map (fn [col] (str (:col_name col))) cols))
     ")
VALUES ("
     (s/join ",
" (map (fn [col] (str "#{" varname "}" "." (:col_name col))) cols))
     ")")))

(defn get-constraints
  [db t]
  (jdbc/query db (str
      get-key-sql
      " WHERE FK.TABLE_NAME = '"
      t "'")))

(defn make-where-filter
  "Create the initial filter wheres, ors, and optional part"
  [filter-cols var]
  (str
   (parser/render (str
                   "<if test=\"[{var}] != '' and [{var}]\" != null\" >"
                   (mybatis-bind "[{var}]")
                   "[{col}] LIKE #{[{var}]}"
                   (make-ors (rest filter-cols) "[{var}]Pattern")
                   "</if>")
                {:col (first filter-cols)
                 :var var}
                {:tag-open \[
                 :tag-close \]})))

(defn mybatis-select-statement
  ([db schema table] (mybatis-select-statement db schema table {}))
  ([db schema table config]
   (let [{:keys [cols mapName queryId where-clause null-conditionals]
          :or {
               cols (query-get-columns db schema table)
               mapName (str (c/->camelCaseString table) "Map")
               queryId (str "get" table)
               where-clause ""
               null-conditionals ""}}
         config
         select-clause (make-select (map :col_name cols))]
     (without-escaping (parser/render main-template
                                      {:cols select-clause
                                       :schema schema
                                       :table table
                                       :map map
                                       :queryId queryId
                                       :where-clause ""
                                       :null-conditionals ""})))))

(defn mybatis-search-statement
  [filter-cols var additional-filters]
  (without-escaping (parser/render main-template
                   {:cols (make-select filter-cols)
                    :schema "Entity"
                    :table "Master "
                    :map "entityMap"
                    :queryId "search"
                    :where-clause (make-where-filter filter-cols var)
                    :null-conditionals (make-extra-filters additional-filters)})))
