module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 6208
# Hash 7484
# Hash 3246
# Hash 9168
# Hash 5659
# Hash 5398
# Hash 9072
# Hash 1872
# Hash 8237
# Hash 1377
# Hash 8550
# Hash 9530
# Hash 6261
# Hash 9866
# Hash 6201
# Hash 2136
# Hash 2268
# Hash 2802
# Hash 5056
# Hash 4267
# Hash 9738
# Hash 1287
# Hash 1845
# Hash 5826
# Hash 1033
# Hash 4405
# Hash 3999
# Hash 8463
# Hash 3468
# Hash 4405
# Hash 8249
# Hash 6905
# Hash 5398
# Hash 6485
# Hash 1633
# Hash 7024
# Hash 2546
# Hash 5640
# Hash 7850
# Hash 3772
# Hash 5757
# Hash 1243
# Hash 2132
# Hash 1099
# Hash 5991
# Hash 2212
# Hash 1275
# Hash 7567
# Hash 2986
# Hash 8138
# Hash 3403
# Hash 5699
# Hash 2452
# Hash 7069
# Hash 7919
# Hash 1859
# Hash 4007
# Hash 9686
# Hash 3707
# Hash 3329
# Hash 2701
# Hash 6864
# Hash 9166
# Hash 8527
# Hash 9314
# Hash 1992
# Hash 1381
# Hash 6411
# Hash 2599
# Hash 8521
# Hash 7672
# Hash 4308
# Hash 9729
# Hash 7537
# Hash 4719
# Hash 1455
# Hash 9017
# Hash 1977
# Hash 5339
# Hash 6287
# Hash 8484
# Hash 2478
# Hash 2172
# Hash 8821
# Hash 1604
# Hash 4331
# Hash 9121