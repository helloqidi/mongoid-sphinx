require 'zlib'

module MongoidSphinx
  module Core
    module String
      def to_crc32
        Zlib.crc32 self
      end
    end
  end
end

class String
  include MongoidSphinx::Core::String
end
