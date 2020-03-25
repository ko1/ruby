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

  # Wait for a message from multiple Ractors.
  #
  # ch, obj = Ractor.select(ch1, ch2, ch3)
  def self.select *ractors
    __builtin_cexpr! %q{
      ractor_select(ec, ractors)
    }
  end

  # Recv from current ractor's incoming channel.
  def self.recv
    __builtin_cexpr! %q{
      ractor_recv(ec, rb_ec_ractor_ptr(ec)->self)
    }
  end

  # Send via current ractor's outgoing channel.
  def self.send obj
    __builtin_cstmt! %q{
      ractor_channel_send(ec, rb_ec_ractor_ptr(ec)->outgoing_channel, obj);
      return self;
    }
  end

  def self.move obj
    __builtin_cstmt! %q{
      ractor_channel_move(ec, rb_ec_ractor_ptr(ec)->outgoing_channel, obj);
      return self;
    }
  end

  # Receive a message from a Racror.
  #
  # If the receiver is current ractor, it received from the incoming channel.
  # If not, it received from the outgoing channel.
  #
  # Example:
  #   r = Ractor.new{ 'oK' }
  #   p r.recv #=> 'ok', received from r's outgoing channel
  def recv
    __builtin_cexpr! %q{
      ractor_recv(ec, self)
    }
  end

  # Send a message to a Ractor.
  #
  # If the receiver is current ractor, send an object to the outgoing channel.
  # If not, send an object to incoming channel.
  #
  # # Example:
  #   r = Ractor.new do
  #     p Ractor.recv #=> 'ok'
  #   end
  #   r.send 'ok' # send to r's incoming channel.
  #
  # # Example:
  # r = Ractor.new do
  #   send 'ok' # send to r's outgoing channel.
  # end
  # r.recv #=> 'ok'
  def send obj
    __builtin_cexpr! %q{
      ractor_send(ec, self, obj)
    }
  end

  alias << send

  # Move a message to a Ractor.
  # Similar to #send, but use "move" semantics.
  # Sent objects can not be accessed from the sent ractor.
  def move obj
    __builtin_cexpr! %q{
      ractor_move(ec, self, obj)
    }
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

  def close
    close_incoming
    close_outgoing
  end
end
