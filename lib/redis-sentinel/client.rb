require "redis"

class Redis::Client
  DEFAULT_FAILOVER_RECONNECT_WAIT_SECONDS = 0.1

  class_eval do
    def initialize_with_sentinel(options={})
      @master_name = fetch_option(options, :master_name)
      @sentinels = fetch_option(options, :sentinels)
      @failover_reconnect_timeout = fetch_option(options, :failover_reconnect_timeout)
      @failover_reconnect_wait = fetch_option(options, :failover_reconnect_wait) ||
                                 DEFAULT_FAILOVER_RECONNECT_WAIT_SECONDS

      initialize_without_sentinel(options)
    end

    alias initialize_without_sentinel initialize
    alias initialize initialize_with_sentinel

    def connect_with_sentinel
      if sentinel?
        auto_retry_with_timeout do
          discover_master
          connect_without_sentinel
        end
      else
        connect_without_sentinel
      end
    end

    alias connect_without_sentinel connect
    alias connect connect_with_sentinel

    def sentinel?
      @master_name && !@sentinels.nil? && !@sentinels.empty?
    end

    def auto_retry_with_timeout(&block)
      deadline = @failover_reconnect_timeout.to_i + Time.now.to_f
      begin
        block.call
      rescue Redis::CannotConnectError
        raise if Time.now.to_f > deadline
        sleep @failover_reconnect_wait
        retry
      end
    end

    def discover_master
      loop do
        begin
          master = get_master_connection_from_sentinel
        rescue Redis::CannotConnectError
          try_next_sentinel
        else
          if master.unknown?
            raise Redis::ConnectionError.new("No master named: #{@master_name}")
          end

          unless master.available?
            raise Redis::CannotConnectError.new("The master: #{@master_name} is currently not available.")
          end

          wait_until_master_failover_complete(master)

          reconfigure_connection_with_new_master(master)

          break
        end
      end
    end

  private

    def try_next_sentinel
      @sentinels << @sentinels.shift
      if @logger && @logger.debug?
        @logger.debug "Trying next sentinel: #{current_sentinel_config[:host]}:#{current_sentinel_config[:port]}"
      end
    end

    def current_sentinel_config
      @sentinels[0]
    end

    def reconfigure_connection_with_new_master(master)
      @options.merge!(:host => master.host, :port => master.port)
    end

    RedisMasterConnection = Struct.new(:host, :port, :is_down, :runid) do
      alias_method :down?, :is_down

      def unknown?
        host.nil? && port == 0
      end

      def available?
        !down? && runid != '?'
      end
    end

    def get_master_connection_from_sentinel
      sentinel = Redis.new(current_sentinel_config)
      host, port = sentinel.sentinel("get-master-addr-by-name", @master_name)
      if host && port
        is_down, runid = sentinel.sentinel("is-master-down-by-addr", host, port)
      end
      sentinel.quit

      RedisMasterConnection.new(host, port.to_i, is_down == 1, runid)
    end

    def wait_until_master_failover_complete(master)
      deadline = @failover_reconnect_timeout.to_i + Time.now.to_f
      until master_failover_complete?(master)
        if  Time.now.to_f < deadline
          sleep @failover_reconnect_wait
        else
          raise Redis::ConnectionError.new("Elected master #{master.host} #{master.port} took too long to leave slave role")
        end
      end
    end

    def master_failover_complete?(master)
      begin
        client = Redis.new(:host => master.host, :port => master.port)
        client.info['role'] == 'master'
      ensure
        client.quit
      end
    end

    def fetch_option(options, key)
      options.delete(key) || options.delete(key.to_s)
    end
  end
end
