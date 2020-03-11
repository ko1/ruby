class Guild
  # Create a new Guild with args and a block.
  # args are passed via incoming channel.
  # A block (Proc) will be isolated (can't acccess to outer variables)
  #
  # A guild has default two channels:
  # an incoming channel and an outgoing channel.
  #
  # Other guilds send objects to the guild via the incoming channel and
  # the guild receives them.
  # The guild send objects via the outgoing channel and other guilds can
  # receive them.
  #
  # The result of the block is sent via the outgoing channel
  # and other 
  #
  # g = Guild.new do
  #   Guild.recv   # recv via g's incoming channel. => 1
  #   Guild.recv   # recv via g's incoming channel. => 2
  #   Guild.send 3 # send via g's outgoing channel.
  #   'ok'       4 # the return value is sent via g's outgoing channel.
  #                # and g's outgoing channel is closed automatically.
  # end
  # g.send 1 # send via g's incoming channel.
  # g <<   2 # << is an alias of `send`.
  # p g.recv   # recv from g's outgoing channel. => 3
  # p g.recv   #                                 => 4
  # p g.recv   # raise Guild::Channel::ClosedError
  def self.new *args, name: nil, &block
    loc = caller_locations(1, 1).first
    loc = "#{loc.path}:#{loc.lineno}"
    __builtin_cexpr! %q{
      guild_create(ec, self, block, args, loc, name)
    }
  end

  # return current Guild
  def self.current
    __builtin_cexpr! %q{
      rb_ec_guild_ptr(ec)->self
    }
  end

  # Wait for multiple channels.
  #
  # ch, obj = Guild.select(ch1, ch2, ch3)
  def self.select *channels
    __builtin_cexpr! %q{
      guild_select(ec, channels)
    }
  end

  # Recv from current guild's incoming channel.
  def self.recv
    __builtin_cexpr! %q{
      guild_channel_recv(ec, rb_ec_guild_ptr(ec)->incoming_channel)
    }
  end

  # Send via current guild's outgoing channel.
  def self.send obj
    __builtin_cexpr! %q{
      guild_channel_send(ec, rb_ec_guild_ptr(ec)->outgoing_channel, obj)
    }
    self
  end

  # Recv via Guild's outgoing channel.
  #
  # Example:
  #   g = Guild.new{ 'oK' }
  #   p g.recv #=> 'ok'
  def recv
    __builtin_cexpr! %q{
      guild_channel_recv(ec, GUILD_PTR(self)->outgoing_channel)
    }
  end

  # Send via Guild's incoming channel.
  #
  # Example:
  #   g = Guild.new do
  #     p Guild.recv #=> 'ok'
  #   end
  #   g.send 'ok'
  def send obj
    __builtin_cexpr! %q{
      guild_channel_send(ec, GUILD_PTR(self)->incoming_channel, obj)
    }
  end

  alias << send

  def move obj
    __builtin_cexpr! %q{
      guild_channel_move(ec, GUILD_PTR(self)->incoming_channel, obj)
    }
    self
  end

  def inspect
    loc  = __builtin_cexpr! %q{ GUILD_PTR(self)->loc }
    name = __builtin_cexpr! %q{ GUILD_PTR(self)->name }
    id   = __builtin_cexpr! %q{ INT2FIX(GUILD_PTR(self)->id) }
    "#<Guild:##{id}#{name ? ' '+name : ''}#{loc ? " " + loc : ''}>"
  end

  class Channel
    # Send an object from this channel from send-side edge.
    # No blocking on current implementation.
    #
    # If the recv-side edge is closed,
    # raises Guild::Channel::ClosedError
    def send obj
      __builtin_guild_channel_send obj
    end

    def move obj
      __builtin_guild_channel_move obj
    end

    # Receive an object from recv-side edge.
    #
    # If no objects are sent, wait until any objects are sent.
    #
    # If send-side edge is closed while blocking,
    # raises Guild::Channel::ClosedError
    def recv
      __builtin_guild_channel_recv
    end

    alias << send

    # Close recv-side edge.
    def close_recv
      __builtin_cexpr! %q{
        guild_channel_close_recv(ec, self);
      }
    end

    # Close send-side edge.
    def close_send
      __builtin_cexpr! %q{
        guild_channel_close_send(ec, self);
      }
    end

    # Close both sides
    def close
      close_recv
      close_send
      nil
    end
  end
end
