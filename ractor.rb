class Ractor
  # Create a new Ractor with args and a block.
  # args are passed via incoming channel.
  # A block (Proc) will be isolated (can't acccess to outer variables)
  #
  # A ractor has default two channels:
  # an incoming channel and an outgoing channel.
  #
  # Other ractors send objects to the ractor via the incoming channel and
  # the ractor receives them.
  # The ractor send objects via the outgoing channel and other ractors can
  # receive them.
  #
  # The result of the block is sent via the outgoing channel
  # and other 
  #
  # g = Ractor.new do
  #   Ractor.recv   # recv via g's incoming channel. => 1
  #   Ractor.recv   # recv via g's incoming channel. => 2
  #   Ractor.send 3 # send via g's outgoing channel.
  #   'ok'       4 # the return value is sent via g's outgoing channel.
  #                # and g's outgoing channel is closed automatically.
  # end
  # g.send 1 # send via g's incoming channel.
  # g <<   2 # << is an alias of `send`.
  # p g.recv   # recv from g's outgoing channel. => 3
  # p g.recv   #                                 => 4
  # p g.recv   # raise Ractor::Channel::ClosedError
  #
  # other options:
  #   name: Ractor's name
  # 
  def self.new *args, name: nil, &block
    b = block # TODO: builtin bug
    loc = caller_locations(1, 1).first
    loc = "#{loc.path}:#{loc.lineno}"
    __builtin_cexpr! %q{
      ractor_create(ec, self, args, b, loc, name)
    }
  end

  # return current Ractor
  def self.current
    __builtin_cexpr! %q{
      rb_ec_ractor_ptr(ec)->self
    }
  end

  # Wait for multiple channels.
  #
  # ch, obj = Ractor.select(ch1, ch2, ch3)
  def self.select *channels
    __builtin_cexpr! %q{
      ractor_select(ec, channels)
    }
  end

  # Recv from current ractor's incoming channel.
  def self.recv
    __builtin_cexpr! %q{
      ractor_channel_recv(ec, rb_ec_ractor_ptr(ec)->incoming_channel)
    }
  end

  # Send via current ractor's outgoing channel.
  def self.send obj
    __builtin_cexpr! %q{
      ractor_channel_send(ec, rb_ec_ractor_ptr(ec)->outgoing_channel, obj)
    }
    self
  end

  def self.move obj
    __builtin_cexpr! %q{
      ractor_channel_move(ec, rb_ec_ractor_ptr(ec)->outgoing_channel, obj)
    }
    self
  end

  # Recv via Ractor's outgoing channel.
  #
  # Example:
  #   g = Ractor.new{ 'oK' }
  #   p g.recv #=> 'ok'
  def recv
    __builtin_cexpr! %q{
      ractor_channel_recv(ec, RACTOR_PTR(self)->outgoing_channel)
    }
  end

  # Send via Ractor's incoming channel.
  #
  # Example:
  #   g = Ractor.new do
  #     p Ractor.recv #=> 'ok'
  #   end
  #   g.send 'ok'
  def send obj
    __builtin_cexpr! %q{
      ractor_channel_send(ec, RACTOR_PTR(self)->incoming_channel, obj)
    }
  end

  alias << send

  def move obj
    __builtin_cexpr! %q{
      ractor_channel_move(ec, RACTOR_PTR(self)->incoming_channel, obj)
    }
    self
  end

  def inspect
    loc  = __builtin_cexpr! %q{ RACTOR_PTR(self)->loc }
    name = __builtin_cexpr! %q{ RACTOR_PTR(self)->name }
    id   = __builtin_cexpr! %q{ INT2FIX(RACTOR_PTR(self)->id) }
    "#<Ractor:##{id}#{name ? ' '+name : ''}#{loc ? " " + loc : ''}>"
  end

  def name
    __builtin_cexpr! %q{ RACTOR_PTR(self)->name }
  end

  class RemoteError
    attr_reader :ractor
  end

  def close_incoming
    __builtin_cexpr! %q{
      ractor_channel_close(ec, RACTOR_PTR(self)->incoming_channel);
    }
  end

  def close_outgoing
    __builtin_cexpr! %q{
      ractor_channel_close(ec, RACTOR_PTR(self)->outgoing_channel);
    }
  end

  class Channel
    # Send an object from this channel from send-side edge.
    # No blocking on current implementation.
    #
    # If the recv-side edge is closed,
    # raises Ractor::Channel::ClosedError
    def send obj
      __builtin_ractor_channel_send obj
    end

    def move obj
      __builtin_ractor_channel_move obj
    end

    # Receive an object from recv-side edge.
    #
    # If no objects are sent, wait until any objects are sent.
    #
    # If send-side edge is closed while blocking,
    # raises Ractor::Channel::ClosedError
    def recv
      __builtin_ractor_channel_recv
    end

    alias << send

    # Close this channel.
    def close
      __builtin_cexpr! %q{
        ractor_channel_close(ec, self);
      }
    end
  end
end
