require_relative '../spec_helper'
require_relative '../fixtures/classes'

describe 'TCPSocket#remote_address' do
  SocketSpecs.each_ip_protocol do |family, ip_address|
    before do
      @server = TCPServer.new(ip_address, 0)
      @host   = @server.connect_address.ip_address
      @port   = @server.connect_address.ip_port
    end

    after do
      @server.close
    end

    describe 'using an explicit hostname' do
      before do
        @sock = TCPSocket.new(@host, @port)
      end

      after do
        @sock.close
      end

      it 'returns an Addrinfo' do
        @sock.remote_address.should.instance_of?(Addrinfo)
      end

      describe 'the returned Addrinfo' do
        it 'uses AF_INET as the address family' do
          @sock.remote_address.afamily.should == family
        end

        it 'uses PF_INET as the protocol family' do
          @sock.remote_address.pfamily.should == family
        end

        it 'uses SOCK_STREAM as the socket type' do
          @sock.remote_address.socktype.should == Socket::SOCK_STREAM
        end

        it 'uses the correct IP address' do
          @sock.remote_address.ip_address.should == @host
        end

        it 'uses the correct port' do
          @sock.remote_address.ip_port.should == @port
        end

        it 'uses 0 as the protocol' do
          @sock.remote_address.protocol.should == 0
        end
      end
    end

    describe 'using an implicit hostname' do
      before do
        @sock = TCPSocket.new(nil, @port)
      end

      after do
        @sock.close
      end

      describe 'the returned Addrinfo' do
        it 'uses the correct IP address' do
          # An implicit (nil) hostname resolves to the loopback of either family
          # and Happy Eyeballs prefers IPv6, so the connection is not guaranteed
          # to land on @host: under parallel runs an unrelated listener on
          # ::1:<same ephemeral port> can be reached instead, making @host a
          # flaky expectation (remote_address would be ::1). Both ends of the
          # same socket always share the family actually connected, so compare
          # against the local end.
          @sock.remote_address.ip_address.should == @sock.local_address.ip_address
        end
      end
    end
  end
end
