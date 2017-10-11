(ns dais.graphql.export-schema
  {:author "Ziyang Hu"}
  (:require [clojure.string :as str]
            [clj-time.core]))

(def section-break "\n\n\n")

(def item-break "\n\n")

(def preamble
  "schema {
  query: QueryRoot
  mutation: MutationRoot
}")

(defn generate-scalar
  [[k _]]
  (str "scalar " (name k)))

(defn generate-description
  [desc]
  (when desc (str "# " desc "\n")))

(defn generate-enum
  [[k {:keys [description values]}]]
  (str (generate-description description)
       "enum "
       (name k)
       " {\n"
       (str/join "\n" (map #(str "  " (name %)) values))
       "\n}"))

(defn generate-type
  [t]
  (cond
    (keyword? t) (name t)
    (string? t) t
    :else (let [[modifier r] t]
            (condp = modifier
              'non-null (str (generate-type r) "!")
              'list (str "[" (generate-type r) "]")))))

(defn generate-field
  ([v] (generate-field v true))
  ([[k {:keys [type args default-value]}] pad?]
   (str (when pad? "  ")
        (name k)
        (when (seq args)
          (str
            "("
            (str/join ", " (map #(generate-field % false) args))
            ")"))
        ": "
        (generate-type type)
        (when-let [default-value-string (cond
                                          (nil? default-value) nil
                                          (keyword? default-value) (name default-value)
                                          :else (pr-str default-value))]
          (str " = " default-value-string)))))

(defn generate-interface
  [[k {:keys [description fields]}]]
  (str (generate-description description)
       "interface "
       (name k)
       " {\n"
       (str/join "\n" (map generate-field fields))
       "\n}"))

(defn generate-object
  [[k {:keys [description implements fields]}]]
  (str (generate-description description)
       "type "
       (name k)
       (when (seq implements)
         (str " implements " (str/join ", " (map name implements)) " "))
       " {\n"
       (str/join "\n" (map generate-field fields))
       "\n}"))

(defn generate-input-object
  [[k {:keys [description implements fields]}]]
  (str (generate-description description)
       "input "
       (name k)
       (when (seq implements)
         (str " implements " (str/join ", " (map name implements)) " "))
       " {\n"
       (str/join "\n" (map generate-field fields))
       "\n}"))

(defn generate-plain-schema
  [{:keys [scalars enums interfaces objects input-objects queries mutations subscriptions]}]
  (str
    section-break
    (str/join item-break (map generate-scalar scalars))
    section-break
    (str/join item-break (map generate-enum enums))
    section-break
    (str/join item-break (map generate-interface interfaces))
    section-break
    (str/join item-break (map generate-object objects))
    section-break
    (str/join item-break (map generate-input-object input-objects))
    section-break
    (generate-object [:QueryRoot {:fields queries}])
    section-break
    (generate-object [:MutationRoot {:fields mutations}])
    section-break
    (generate-object [:SubscriptionRoot {:fields subscriptions}])
    section-break
    preamble))
