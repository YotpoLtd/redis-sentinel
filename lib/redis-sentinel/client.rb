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
          host, port = get_master_connection_from_sentinel
        rescue Redis::CannotConnectError
          try_next_sentinel
        else
          if !host && !port
            raise Redis::ConnectionError.new("No master named: #{@master_name}")
          end

          wait_until_master_failover_complete(host, port)

          reconfigure_connection_with_new_master(host, port)

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

    def reconfigure_connection_with_new_master(host, port)
      @options.merge!(:host => host, :port => port.to_i)
    end

    def get_master_connection_from_sentinel
      sentinel = Redis.new(current_sentinel_config)
      host, port = sentinel.sentinel("get-master-addr-by-name", @master_name)
      sentinel.quit
      return [host, port]
    end

    def wait_until_master_failover_complete(host, port)
      deadline = @failover_reconnect_timeout.to_i + Time.now.to_f
      until master_failover_complete?(host, port.to_i)
        if  Time.now.to_f < deadline
          sleep @failover_reconnect_wait
        else
          raise Redis::ConnectionError.new("Elected master #{host} #{port} took too long to leave slave role")
        end
      end
    end

    def master_failover_complete?(host, port)
      begin
        client = Redis.new(:host => host, :port => port)
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
