(ns onyx.messaging.aeron-sub-tester
  (:require [onyx.messaging.aeron :as aeron :refer [aeron]]
            [onyx.messaging.protocol-aeron :as protocol-aeron]
            [clojure.set :refer [intersection]]
            [onyx.system :as system]
            [onyx.extensions :as ext]
            [onyx.api]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [onyx.test-helper :refer [load-config]]
            [onyx.types :refer [map->Leaf map->Ack]]
            [com.stuartsierra.component :as component]))

(def id (java.util.UUID/randomUUID))

(def config (load-config))

(def sample-commands '({:payload [#onyx.types.Leaf{:message {:some-key '(\J 2 :l+:?s2O:n1L)}, :id #uuid "a83ef65a-4823-418a-9442-50c169188f70", :acker-id #uuid "43495c46-a0de-420a-9542-419ba0f17e41", :completion-id #uuid "bf31209e-56a5-4c37-a481-5ce730d98f05", :ack-val 2, :ack-vals nil, :route nil, :routes nil, :hash-group nil}], :command :messages} {:payload [#onyx.types.Ack{:id #uuid "dbda2abf-8e1a-425b-9550-526b3d746c12", :completion-id #uuid "816cc572-39d6-4a3f-abe9-d9de70f3573e", :ack-val 1, :timestamp nil}], :command :acks} {:payload #uuid "920ce2b5-87b0-44da-91b9-5ac0bfa6459d", :command :retry} {:payload [#onyx.types.Leaf{:message {:some-key []}, :id #uuid "f99f0edc-e28b-4693-9473-b646f194b541", :acker-id #uuid "bc6c9afe-0562-45f6-9653-917002e2364d", :completion-id #uuid "7a19b75b-459d-4601-a9b5-763636d7638a", :ack-val 1, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key {}}, :id #uuid "67a4ccd1-b3ef-4bf1-b61c-527ae980ddc0", :acker-id #uuid "a54abd2e-0882-41e5-9cca-d57d8def7c7d", :completion-id #uuid "a45c2525-6727-43d9-b119-87b71b35cefd", :ack-val 0, :ack-vals nil, :route nil, :routes nil, :hash-group nil}], :command :messages} {:payload [#onyx.types.Ack{:id #uuid "6a7e51eb-9fdd-4784-ba13-ff828dfa5a38", :completion-id #uuid "31e2f553-481a-4bea-b135-5ca486658bfc", :ack-val 1, :timestamp nil}], :command :acks} {:payload #uuid "5c4c499f-6cbb-4b37-8a86-0c9df37a451c", :command :complete} {:payload #uuid "eaf67aaf-dd07-460e-acc1-8db5aa7ba312", :command :retry} {:payload [#onyx.types.Ack{:id #uuid "386cc932-316e-458e-a42f-8b702079c2e8", :completion-id #uuid "c94ee3bb-3d59-438f-a80c-0f4ab5b8314b", :ack-val -4, :timestamp nil} #onyx.types.Ack{:id #uuid "b23d46a7-00c4-44a6-820b-688d0cf87a15", :completion-id #uuid "743823d5-8d9a-4d99-a007-84fca4d925c6", :ack-val 0, :timestamp nil} #onyx.types.Ack{:id #uuid "a1cce535-5344-401c-a598-194df93ad3e3", :completion-id #uuid "9bf81f78-9b9f-4e55-af0b-04861b3b7fbd", :ack-val -3, :timestamp nil}], :command :acks} {:payload [#onyx.types.Leaf{:message {:some-key {'() '()}}, :id #uuid "53cf4266-b260-41a1-a56d-d59e275dba5c", :acker-id #uuid "8de9945d-2be2-4c81-8298-eb7c69eaa338", :completion-id #uuid "8f32a655-196d-4cdb-8fdb-6d27476428da", :ack-val 3, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key {:fep0T3W.?pw0.!w+x!.Jp.*Zv5X/yw__Z+_* false, "?&7" :?__K+:L-Y:!051f, -1 -6, _1?/.66! 1/5, 7 R?3c1G, true -3, j5_v.*63P1.iZ+yw?x.e!Jfv/os9v8 :l?_4}}, :id #uuid "4ac84151-ce25-4edd-9d89-006bce295162", :acker-id #uuid "95aaa536-1317-4f82-a65a-1c8d24a1d67e", :completion-id #uuid "a69f2b60-af1c-42c4-bca5-28dc8c817504", :ack-val -6, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key {}}, :id #uuid "0a532209-9d8f-4054-a579-6543246a9297", :acker-id #uuid "bf9ace7d-d82b-4c09-a3c9-dc658feaad8f", :completion-id #uuid "0f75cc56-736c-4030-909a-50c0514c6c79", :ack-val 5, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key '()}, :id #uuid "b0b44c22-12bb-47f0-9792-49388e5928b5", :acker-id #uuid "7477ba5d-48a4-42ba-8156-b69d278a226a", :completion-id #uuid "d4419e7c-6aeb-47e5-ac27-fd63fa42da8d", :ack-val -2, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key [['("") {}] '([] '(_h0))]}, :id #uuid "5eda06b6-59c3-4b23-ab46-19fb099a3d35", :acker-id #uuid "383e317b-fda9-48bf-a0fe-138a060bead9", :completion-id #uuid "4e03eef6-108a-4ece-b0e7-dec64b3a04f0", :ack-val 4, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key [C?Z*LV1N true \9 !v0s+o]}, :id #uuid "b28b4f2e-cdac-4bc2-ac2e-0997f4578638", :acker-id #uuid "d453433d-0372-4eb8-a12b-2727bee49619", :completion-id #uuid "4159c428-9987-46dd-b9b4-074d290d6df1", :ack-val -5, :ack-vals nil, :route nil, :routes nil, :hash-group nil} #onyx.types.Leaf{:message {:some-key []}, :id #uuid "cef8676c-f872-40f1-94e4-a23bd6df5ca7", :acker-id #uuid "5579cf78-f813-4972-bc01-b5e6a20bd29f", :completion-id #uuid "9f3bb97c-4fe2-40b5-bdbd-2e3a0f498fea", :ack-val 5, :ack-vals nil, :route nil, :routes nil, :hash-group nil}], :command :messages} {:payload [#onyx.types.Ack{:id #uuid "1d0438af-1ac7-494b-a79c-65447db46706", :completion-id #uuid "45a277ad-e9f9-4b60-af89-75f63615a6ed", :ack-val 3, :timestamp nil} #onyx.types.Ack{:id #uuid "bceca7e3-5581-4af5-b857-d3184abb2c24", :completion-id #uuid "a3edaef7-3f3d-4e19-bf10-cafbd3fd24f6", :ack-val -5, :timestamp nil} #onyx.types.Ack{:id #uuid "9fb5c5db-94b6-4521-aef5-512f13a53c4e", :completion-id #uuid "9b601001-af08-44b8-97a9-c15792efce5d", :ack-val 4, :timestamp nil} #onyx.types.Ack{:id #uuid "dc3ba690-1c30-4f3e-8c8b-c7bd2f80b8a6", :completion-id #uuid "dc4d6d79-88cf-437d-9760-aaf38c3f8382", :ack-val -9, :timestamp nil} #onyx.types.Ack{:id #uuid "673d28db-6d50-4d52-8f9a-3e2957c1004c", :completion-id #uuid "1dea9c3b-48c1-4f10-ae0c-c8d2bd22d823", :ack-val 2, :timestamp nil} #onyx.types.Ack{:id #uuid "6e8bee4e-e534-4ac3-b276-eb7bbc867e78", :completion-id #uuid "339a73bb-a90f-43aa-931d-ea887e8938b5", :ack-val 8, :timestamp nil} #onyx.types.Ack{:id #uuid "37df38bd-0236-4fe1-8f11-43d7ab431530", :completion-id #uuid "8d3a39e6-4d69-490b-8175-2a7816f46e72", :ack-val 2, :timestamp nil} #onyx.types.Ack{:id #uuid "9c38e34c-3a28-49a2-b852-276a8a9db93c", :completion-id #uuid "f0d1240d-a6f5-42fe-a10a-526824b4cdf8", :ack-val 6, :timestamp nil}], :command :acks}))

(defn handle-sent-message [received] 
  (fn [inbound-ch decompress-f buffer offset length header]
    (let [messages (protocol-aeron/read-messages-buf decompress-f buffer offset length)]
      (swap! received update-in [:messages] into messages))))

(defn handle-aux-message [received] 
  (fn [daemon release-ch retry-ch buffer offset length header]
    (let [msg-type (protocol-aeron/read-message-type buffer offset)
          offset-rest (long (inc offset))] 
      (cond (= msg-type protocol-aeron/ack-msg-id)
            (let [ack (protocol-aeron/read-acker-message buffer offset-rest)]
              (swap! received update-in [:acks] conj ack))

            (= msg-type protocol-aeron/completion-msg-id)
            (let [completion-id (protocol-aeron/read-completion buffer offset-rest)]
              (swap! received update-in [:complete] conj completion-id))

            (= msg-type protocol-aeron/retry-msg-id)
            (let [retry-id (protocol-aeron/read-retry buffer offset-rest)]
              (swap! received update-in [:retry] conj retry-id))))))


(defn test-send-commands-aeron [addr port peer-group commands]
  (let [send-messenger (component/start (aeron peer-group))]

    (try 
      (let [send-link (ext/connect-to-peer send-messenger nil {:aeron/external-addr addr
                                                               :aeron/port port})]
          (reduce (fn [_ command] 
                    (case (:command command)
                      :messages (ext/send-messages send-messenger nil send-link (:payload command))              
                      :complete (ext/internal-complete-message send-messenger nil (:payload command) send-link)              
                      :retry (ext/internal-retry-message send-messenger nil (:payload command) send-link)              
                          :acks (ext/internal-ack-messages send-messenger nil send-link (:payload command)))) 
                      nil commands)

              (Thread/sleep 1500)
              (ext/close-peer-connection send-messenger nil send-link)
              (println "All sent!"))
            (finally 
              (component/stop send-messenger)))))

(defn test-receive-commands-aeron [addr port peer-group]
    (let [received (atom {})]
      (with-redefs [aeron/handle-sent-message (handle-sent-message received)
                    aeron/handle-aux-message (handle-aux-message received)] 
        (let [recv-messenger (component/start (aeron peer-group))]

          (try 
            (let [_ (ext/open-peer-site recv-messenger {:onyx.messaging/bind-addr addr
                                                        :aeron/port port})]
              

              (Thread/sleep 60000)
              (println "Received! " @received))
            (finally 
              (component/stop recv-messenger)))))))

(defn -main [type addr port embedded]
  (let [peer-config-aeron (assoc (:peer-config config) 
                                 :onyx/id id 
                                 :onyx.messaging.aeron/embedded-driver? (if (= embedded "true")
                                                                          true
                                                                          false)
                                 :onyx.messaging/bind-addr addr

                                 :onyx.messaging/impl :aeron)
        peer-group (onyx.api/start-peer-group peer-config-aeron)] 
    (try 
      (case type
        "receive" (test-receive-commands-aeron addr (Integer/parseInt port) peer-group)
        "send" (test-send-commands-aeron addr (Integer/parseInt port) peer-group sample-commands))    
      (finally (onyx.api/shutdown-peer-group peer-group)))))
