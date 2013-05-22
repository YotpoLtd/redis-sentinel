require "spec_helper"

describe Redis::Client do
  let(:info) {{'role' => 'master'}}
  let(:redis) { mock("Redis", :info => info,
                              :quit => nil)}

  subject { Redis::Client.new(:master_name => "master",
                              :sentinels => [{:host => "localhost", :port => 26379},
                                             {:host => "localhost", :port => 26380}]) }

  before { Redis.stub(:new).and_return(redis) }

  context "#sentinel?" do
    it "should be true if passing sentiels and master_name options" do
      expect(Redis::Client.new(:master_name => "master", :sentinels => [{:host => "localhost", :port => 26379}, {:host => "localhost", :port => 26380}])).to be_sentinel
    end

    it "should not be true if not passing sentinels and maser_name options" do
      expect(Redis::Client.new).not_to be_sentinel
    end

    it "should not be true if passing sentinels option but not master_name option" do
      expect(Redis::Client.new(:sentinels => [{:host => "localhost", :port => 26379}, {:host => "localhost", :port => 26380}])).not_to be_sentinel
    end

    it "should not be true if passing master_name option but not sentinels option" do
      expect(Redis::Client.new(:master_name => "master")).not_to be_sentinel
    end
  end

  context "#discover_master" do
    let(:get_master_addr_by_name) { ["remote.server", 8888] }
    let(:is_master_down_by_addr) { [0, "abc"] }

    before(:each) do
      redis.stub(:sentinel).and_return do |command, *args|
        case command
        when "get-master-addr-by-name"
          get_master_addr_by_name
        when "is-master-down-by-addr"
          is_master_down_by_addr
        end
      end
    end

    it "gets the current master" do
      redis.should_receive(:sentinel).with("get-master-addr-by-name", "master")
      subject.discover_master
    end

    it "checks to see if the current master is down" do
      redis.should_receive(:sentinel).with("is-master-down-by-addr", "remote.server", 8888)
      subject.discover_master
    end

    it "should update options" do
      subject.discover_master
      subject.host.should == "remote.server"
      subject.port.should == 8888
    end

    it "confirms that the elected master is actually master" do
      redis.should_receive(:info)

      subject.discover_master
    end

    context "no master by that name" do
      let(:get_master_addr_by_name) { [nil, nil] }

      it "raises a connection error" do
        expect { subject.discover_master }.to raise_error(Redis::ConnectionError)
      end
    end

    context "master takes too long to become master" do
      let(:info) {{'role' => 'slave'}}

      it "raises a connection error" do
        expect { subject.discover_master }.to raise_error(Redis::ConnectionError)
      end
    end

    context "current master is down" do
      let(:is_master_down_by_addr) { [1, "abc"] }

      it "raises a connection error" do
        expect { subject.discover_master }.to raise_error(Redis::CannotConnectError)
      end
    end
  end

  context "#auto_retry_with_timeout" do
    context "no failover reconnect timeout set" do
      subject { Redis::Client.new }

      it "does not sleep" do
        subject.should_not_receive(:sleep)
        expect do
          subject.auto_retry_with_timeout { raise Redis::CannotConnectError }
        end.to raise_error(Redis::CannotConnectError)
      end
    end

    context "the failover reconnect timeout is set" do
      subject { Redis::Client.new(:failover_reconnect_timeout => 3) }

      before(:each) do
        subject.stub(:sleep)
      end

      it "only raises after the failover_reconnect_timeout" do
        called_counter = 0
        Time.stub(:now).and_return(100, 101, 102, 103, 104, 105)

        begin
          subject.auto_retry_with_timeout do
            called_counter += 1
            raise Redis::CannotConnectError
          end
        rescue Redis::CannotConnectError
        end

        called_counter.should == 4
      end

      it "sleeps the default wait time" do
        Time.stub(:now).and_return(100, 101, 105)
        subject.should_receive(:sleep).with(0.1)
        begin
          subject.auto_retry_with_timeout { raise Redis::CannotConnectError }
        rescue Redis::CannotConnectError
        end
      end

      it "does not catch other errors" do
        subject.should_not_receive(:sleep)
        expect do
          subject.auto_retry_with_timeout { raise Redis::ConnectionError }
        end.to raise_error(Redis::ConnectionError)
      end

      context "configured wait time" do
        subject { Redis::Client.new(:failover_reconnect_timeout => 3,
                                    :failover_reconnect_wait => 0.01) }

        it "uses the configured wait time" do
          Time.stub(:now).and_return(100, 101, 105)
          subject.should_receive(:sleep).with(0.01)
          begin
            subject.auto_retry_with_timeout { raise Redis::CannotConnectError }
          rescue Redis::CannotConnectError
          end
        end
      end
    end
  end
end
