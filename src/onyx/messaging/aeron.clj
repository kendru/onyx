(ns ^:no-doc onyx.messaging.aeron
  (:require [clojure.core.async :refer [chan >!! <!! alts!! timeout close! sliding-buffer]]
            [com.stuartsierra.component :as component]
            [taoensso.timbre :refer [fatal] :as timbre]
            [onyx.messaging.protocol-aeron :as protocol]
            [onyx.messaging.common :as common]
            [onyx.extensions :as extensions]
            [onyx.compression.nippy :refer [compress decompress]]
            [onyx.static.default-vals :refer [defaults arg-or-default]])
  (:import [uk.co.real_logic.aeron Aeron FragmentAssembler]
           [uk.co.real_logic.aeron Aeron$Context]
           [uk.co.real_logic.aeron Publication]
           [uk.co.real_logic.aeron.driver MediaDriver MediaDriver$Context ThreadingMode]
           [uk.co.real_logic.aeron.logbuffer FragmentHandler]
           [uk.co.real_logic.agrona.concurrent UnsafeBuffer]
           [uk.co.real_logic.agrona CloseHelper]
           [uk.co.real_logic.agrona ErrorHandler]
           [uk.co.real_logic.agrona.concurrent IdleStrategy BackoffIdleStrategy BusySpinIdleStrategy]
           [java.util.function Consumer]
           [java.util.concurrent TimeUnit]))

(def send-stream-id 1)

(defn aeron-channel [addr port]
  (format "udp://%s:%s" addr port))

(defrecord AeronConnection 
  [peer-group messaging-group short-circuitable? publications connections virtual-peers acking-daemon acking-ch 
   send-idle-strategy compress-f inbound-ch release-ch retry-ch]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron")
    (let [config (:config peer-group)
          messaging-group (:messaging-group peer-group)
          publications (:publications messaging-group)
          connections (:connections messaging-group)
          virtual-peers (:virtual-peers messaging-group)
          inbound-ch (:inbound-ch (:messenger-buffer component))
          external-channel (:external-channel messaging-group)
          short-circuitable? (if (arg-or-default :onyx.messaging/allow-short-circuit? config)
                               (fn [channel] (= channel external-channel))
                               (constantly false))
          release-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/release-ch-buffer-size config)))
          retry-ch (chan (sliding-buffer (arg-or-default :onyx.messaging/retry-ch-buffer-size config)))
          acking-ch (:acking-ch (:acking-daemon component))
          send-idle-strategy (:send-idle-strategy messaging-group)
          compress-f (:compress-f (:messaging-group peer-group))
          multiplex-id (atom nil)]
      (assoc component 
             :messaging-group messaging-group
             :short-circuitable? short-circuitable?
             :publications publications
             :connections connections
             :multiplex-id multiplex-id
             :virtual-peers virtual-peers
             :send-idle-strategy send-idle-strategy
             :compress-f compress-f
             :acking-ch acking-ch
             :inbound-ch inbound-ch
             :release-ch release-ch
             :retry-ch retry-ch)))

  (stop [{:keys [release-ch retry-ch virtual-peers multiplex-id] :as component}]
    (taoensso.timbre/info "Stopping Aeron")
    (try 
      ;;; TODO; could handle the inbound-ch in a similar way - rather than using messenger-buffer
      (close! release-ch)
      (close! retry-ch)
      (when @multiplex-id 
        (swap! virtual-peers dissoc @multiplex-id))
      (catch Throwable e (fatal e)))
    (assoc component
           :send-idle-strategy nil 
           :short-circuitable? nil
           :publications nil
           :connections nil
           :virtual-peers nil
           :multiplex-id nil
           :compress-f nil :decompress-f nil 
           :inbound-ch nil :release-ch nil :retry-ch nil )))

(defrecord PeerChannels [acking-ch inbound-ch release-ch retry-ch])

(defmethod extensions/open-peer-site AeronConnection
  [{:keys [virtual-peers multiplex-id acking-ch inbound-ch release-ch retry-ch] :as messenger} 
   {:keys [aeron/id]}]
  (reset! multiplex-id id)
  (swap! virtual-peers assoc id (->PeerChannels acking-ch inbound-ch release-ch retry-ch))) 

(def no-op-error-handler
  (reify ErrorHandler 
    (onError [this x] (taoensso.timbre/warn x))))

(defn fragment-data-handler [f]
  (FragmentAssembler. 
    (reify FragmentHandler 
      (onFragment [this buffer offset length header]
        (f buffer offset length header)))))

(defn backoff-strategy [strategy]
  (case strategy
    :busy-spin (BusySpinIdleStrategy.)
    :low-restart-latency (BackoffIdleStrategy. 100
                                               10
                                               (.toNanos TimeUnit/MICROSECONDS 1)
                                               (.toNanos TimeUnit/MICROSECONDS 100))
    :high-restart-latency (BackoffIdleStrategy. 1000
                                                100
                                                (.toNanos TimeUnit/MICROSECONDS 10)
                                                (.toNanos TimeUnit/MICROSECONDS 1000))))

(defn consumer [handler ^IdleStrategy idle-strategy limit]
  (reify Consumer 
    (accept [this subscription]
      (while (not (Thread/interrupted))
        (let [fragments-read (.poll ^uk.co.real_logic.aeron.Subscription subscription ^FragmentHandler handler ^int limit)]
          (.idle idle-strategy fragments-read))))))

(defn handle-message [decompress-f virtual-peers buffer offset length header]
  ;;; Problem, all de-serialization is now done in a single subscriber thread
  (let [msg-type (protocol/read-message-type buffer offset)
        offset (inc ^long offset)
        peer-id (protocol/read-vpeer-id buffer offset)
        offset (+ offset protocol/short-size)] 
    (cond (= msg-type protocol/ack-msg-id)
          (let [ack (protocol/read-acker-message buffer offset)]
            (when-let [chs (get @virtual-peers peer-id)] 
              (>!! (:acking-ch chs) ack)))

          (= msg-type protocol/messages-msg-id)
          (let [segments (protocol/read-messages-buf decompress-f buffer offset length)]
            (when-let [chs (get @virtual-peers peer-id)] 
              (let [inbound-ch (:inbound-ch chs)] 
                (doseq [segment segments]
                  (>!! inbound-ch segment)))))

          (= msg-type protocol/completion-msg-id)
          (let [completion-id (protocol/read-completion buffer offset)]
            (when-let [chs (get @virtual-peers peer-id)] 
              (>!! (:release-ch chs) completion-id)))

          (= msg-type protocol/retry-msg-id)
            (let [retry-id (protocol/read-retry buffer offset)]
              (when-let [chs (get @virtual-peers peer-id)] 
                (>!! (:retry-ch chs) retry-id))))))

(defn start-subscriber! [bind-addr port virtual-peers decompress-f idle-strategy]
  (let [ctx (.errorHandler (Aeron$Context.) no-op-error-handler)
        conn (Aeron/connect ctx)
        channel (aeron-channel bind-addr port)
        handler (fragment-data-handler 
                  (fn [buffer offset length header] 
                    (handle-message decompress-f virtual-peers
                                    buffer offset length header)))

        subscription (.addSubscription conn channel send-stream-id)
        subscriber-fut (future (try (.accept ^Consumer (consumer handler idle-strategy 10) subscription) 
                                    (catch Throwable e (fatal e))))]
    {:conn conn :subscription subscription :subscriber-fut subscriber-fut}))

(defrecord TrackedPublication [publication last-used])

(defn get-publication [messenger channel]
  ;; FIXME, race condition may cause two publications to be created
  ;; same goes for operation/peer-link
  (if-let [pub (get @(:publications messenger) channel)]
    (do 
      (reset! (:last-used pub) (System/currentTimeMillis))
      (:publication pub))
    (let [conn (Aeron/connect (.errorHandler (Aeron$Context.) no-op-error-handler))
          pub (.addPublication conn channel send-stream-id)]
      (do (swap! (:publications messenger) assoc channel (->TrackedPublication pub (atom (System/currentTimeMillis))))
          (swap! (:connections messenger) assoc channel conn)
          pub))))

(defn opts->port [opts]
  (or (first (common/allowable-ports opts))
      (throw 
        (ex-info "Couldn't assign port - ran out of available ports.
                 Available ports can be configured in the peer-config.
                 e.g. {:onyx.messaging/peer-ports [40000, 40002],
                 :onyx.messaging/peer-port-range [40200 40260]}"))))


;; FIXME: gc'ing publications could be racy if a publication is grabbed 
;; just as it is being gc'd - though it is unlikely
(defn gc-publications [publications connections opts]
  (let [interval (arg-or-default :onyx.messaging/peer-link-gc-interval opts)
        idle (arg-or-default :onyx.messaging/peer-link-idle-timeout opts)]
    (loop []
      (try
        (Thread/sleep interval)
        (let [t (System/currentTimeMillis)
              snapshot @publications
              to-remove (map first 
                             (filter (fn [[k v]] (>= (- t ^long @(:last-used v)) idle)) 
                                     snapshot))]
          (doseq [k to-remove]
            (let [pub (:publication (snapshot k))
                  conn (@connections k)] 
              (swap! publications dissoc k)
              (swap! connections dissoc k)
              (.close ^Publication pub)
              (.close ^Aeron conn))))
        (catch InterruptedException e
          (throw e))
        (catch Throwable e
          (fatal e)))
      (recur))))

(defrecord AeronPeerGroup [opts publications connections compress-f decompress-f send-idle-strategy]
  component/Lifecycle
  (start [component]
    (taoensso.timbre/info "Starting Aeron Peer Group")
    (let [embedded-driver? (arg-or-default :onyx.messaging.aeron/embedded-driver? opts)
          ;; TODO: evaluate whether we should be using the official
          ;; launchEmbedded feature in media driver, rather than rolling our own
          media-driver-context (if embedded-driver? 
                                 (doto (MediaDriver$Context.)))
          media-driver (if embedded-driver?
                         (MediaDriver/launch media-driver-context))

          bind-addr (common/bind-addr opts)
          external-addr (common/external-addr opts)
          port (opts->port opts)
          external-channel (aeron-channel external-addr port)
          poll-idle-strategy-config (arg-or-default :onyx.messaging.aeron/poll-idle-strategy opts) 
          offer-idle-strategy-config (arg-or-default :onyx.messaging.aeron/offer-idle-strategy opts) 
          send-idle-strategy (backoff-strategy poll-idle-strategy-config)
          receive-idle-strategy (backoff-strategy offer-idle-strategy-config)
          compress-f (or (:onyx.messaging/compress-fn opts) compress)
          decompress-f (or (:onyx.messaging/decompress-fn opts) decompress)
          virtual-peers (atom {})
          publications (atom {})
          connections (atom {})
          subscriber (start-subscriber! bind-addr port virtual-peers decompress-f receive-idle-strategy)
          pub-gc-thread (future (gc-publications publications connections opts))]
      (assoc component 
             :pub-gc-thread pub-gc-thread
             :bind-addr bind-addr
             :external-addr external-addr
             :external-channel external-channel
             :port port
             :media-driver-context media-driver-context
             :media-driver media-driver 
             :publications publications
             :connections connections
             :virtual-peers virtual-peers
             :compress-f compress-f
             :decompress-f decompress-f
             :port port
             :send-idle-strategy send-idle-strategy
             :subscriber subscriber)))

  (stop [{:keys [media-driver media-driver-context subscriber publications connections] :as component}]
    (taoensso.timbre/info "Stopping Aeron Peer Group")
    (future-cancel (:subscriber-fut subscriber))
    (future-cancel (:pub-gc-thread component))
    (.close ^uk.co.real_logic.aeron.Subscription (:subscription subscriber))
    (.close ^Aeron (:conn subscriber))
    (doseq [pub (vals @publications)]
      (.close ^Publication (:publication pub)))
    (doseq [conn (vals @connections)]
      (.close ^Aeron conn))
    (when media-driver (.close ^MediaDriver media-driver))
    (when media-driver-context (.deleteAeronDirectory ^MediaDriver$Context media-driver-context))
    (assoc component 
           :pub-gc-thread nil
           :media-driver nil :publications nil :virtual-peers nil
           :media-driver-context nil
           :external-channel nil
           :compress-f nil :decompress-f nil :send-idle-strategy nil
           :subscriber nil)))

(Math/pow 2 16)
(mod -30000 65536)

(defn free-consistent-hash [existing v]
  (let [initial-set (set (range 1 2) #_(range -32767 32766))] 
    (loop [hs (hash (java.util.UUID/randomUUID))]
      (let [mod-signed (- (mod hs 65536)
                          32768)] 
        (if (initial-set mod-signed)
          (recur (hash hs))
          mod-signed)))))

(map short 
     (for [v (range 1000000)]
       (free-consistent-hash nil v)))

(defn aeron-peer-group [opts]
  (map->AeronPeerGroup {:opts opts}))

(def possible-ids 
  (set (map short (range -32768 32768))))

(defn choose-id [used]
  (first (clojure.set/difference possible-ids used)))

(defmethod extensions/assign-site-resources :aeron
  [replica peer-site peer-sites]
  ;;; Assigns a unique id to each peer so that messages do not need
  ;;; to send the entire peer-id in a payload, saving 14 bytes per
  ;;; message
  (let [used-ids (->> (vals peer-sites) 
                      (filter 
                        (fn [s]
                          (= (:aeron/external-addr peer-site) 
                             (:aeron/external-addr s))))
                      (map :aeron/id)
                      set)
        id (choose-id used-ids)]
    (when-not id
      (throw (ex-info "Couldn't assign id. Ran out of aeron ids. This should only happen if more than 65356 virtual peers have been started up on a single external addr")))
    {:aeron/id id}))

(defmethod extensions/get-peer-site :aeron
  [replica peer]
  (get-in replica [:peer-sites peer :aeron/external-addr]))

(defn aeron [peer-group]
  (map->AeronConnection {:peer-group peer-group}))

(defmethod extensions/peer-site AeronConnection
  [messenger]
  {:aeron/external-addr (:external-addr (:messaging-group messenger))
   :aeron/port (:port (:messaging-group messenger))})

(defrecord AeronPeerConnection [channel id])

(defmethod extensions/connect-to-peer AeronConnection
  [messenger peer-id event {:keys [aeron/external-addr aeron/port aeron/id]}]
  (->AeronPeerConnection (aeron-channel external-addr port) id))

(defmethod extensions/receive-messages AeronConnection
  [messenger {:keys [onyx.core/task-map] :as event}]
  (let [ch (:inbound-ch messenger)
        batch-size (long (:onyx/batch-size task-map))
        ms (arg-or-default :onyx/batch-timeout task-map)
        timeout-ch (timeout ms)]
    (loop [segments [] i 0]
      (if (< i batch-size)
        (if-let [v (first (alts!! [ch timeout-ch]))]
          (recur (conj segments v) (inc i))
          segments)
        segments))))

(defn short-circuit-ch [messenger id ch-k]
  (-> messenger 
      :virtual-peers 
      deref 
      (get id)
      ch-k))

(defn send-messages-short-circuit [ch batch]
  (doseq [segment batch]
    (>!! ch segment)))

(defmethod extensions/send-messages AeronConnection
  [messenger event {:keys [id channel]} batch]
  (if ((:short-circuitable? messenger) channel) 
    (send-messages-short-circuit (short-circuit-ch messenger id :inbound-ch) batch)
    (let [pub ^Publication (get-publication messenger channel)
          [len unsafe-buffer] (protocol/build-messages-msg-buf (:compress-f messenger) id batch)
          offer-f (fn [] (.offer pub unsafe-buffer 0 len))
          idle-strategy (:send-idle-strategy messenger)]
      ;;; TODO: offer will return a particular code when the publisher is down
      ;;; we should probably re-restablish the publication in this case
      (while (neg? ^long (offer-f))
        (.idle ^IdleStrategy idle-strategy 0)))))

(defn ack-messages-short-circuit [ch acks]
  (doseq [ack acks]
    (>!! ch ack)))

(defmethod extensions/internal-ack-messages AeronConnection
  [messenger event {:keys [id channel]} acks]
  (if ((:short-circuitable? messenger) channel) 
    (ack-messages-short-circuit (short-circuit-ch messenger id :acking-ch) acks)
    (let [pub ^Publication (get-publication messenger channel)
          idle-strategy (:send-idle-strategy messenger)] 
      (doseq [ack acks] 
        (let [unsafe-buffer (protocol/build-acker-message id (:id ack) (:completion-id ack) (:ack-val ack))
              offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/ack-msg-length))]
          (while (neg? ^long (offer-f))
            (.idle ^IdleStrategy idle-strategy 0)))))))

(defn complete-message-short-circuit [ch completion-id]
  (>!! ch completion-id))

(defmethod extensions/internal-complete-message AeronConnection
  [messenger event completion-id {:keys [id channel]}]
  (if ((:short-circuitable? messenger) channel) 
    (complete-message-short-circuit (short-circuit-ch messenger id :release-ch) completion-id)
    (let [idle-strategy (:send-idle-strategy messenger)
          pub ^Publication (get-publication messenger channel)
          unsafe-buffer (protocol/build-completion-msg-buf id completion-id)
          offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/completion-msg-length))]
      (while (neg? ^long (offer-f))
        (.idle ^IdleStrategy idle-strategy 0)))))

(defn retry-message-short-circuit [ch retry-id]
  (>!! ch retry-id))

(defmethod extensions/internal-retry-message AeronConnection
  [messenger event retry-id {:keys [id channel]}]
  (if ((:short-circuitable? messenger) channel) 
    (retry-message-short-circuit (short-circuit-ch messenger id :retry-ch) retry-id)
    (let [idle-strategy (:send-idle-strategy messenger)
          pub ^Publication (get-publication messenger channel)
          unsafe-buffer (protocol/build-retry-msg-buf id retry-id)
          offer-f (fn [] (.offer pub unsafe-buffer 0 protocol/retry-msg-length))]
      (while (neg? ^long (offer-f))
        (.idle ^IdleStrategy idle-strategy 0)))))

(defmethod extensions/close-peer-connection AeronConnection
  [messenger event peer-link]
  {})
