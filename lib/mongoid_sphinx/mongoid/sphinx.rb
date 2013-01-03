# MongoidSphinx, a full text indexing extension for MongoDB/Mongoid using
# Sphinx.

module Mongoid
  module Sphinx
    extend ActiveSupport::Concern
    included do
      unless defined?(SPHINX_TYPE_MAPPING)
        SPHINX_TYPE_MAPPING = {
          'Date' => 'timestamp',
          'DateTime' => 'timestamp',
          'Time' => 'timestamp',
          'Float' => 'float',
          'Integer' => 'int',
          'BigDecimal' => 'float',
          'Boolean' => 'bool',
					'String' => 'string',
					'Object' => 'string',
					'Array' => 'string'
        }
      end

      cattr_accessor :search_fields
      cattr_accessor :search_attributes
      cattr_accessor :index_options
    end

    module ClassMethods

      def search_index(options={})
        self.search_fields = options[:fields]
        self.search_attributes = {}
        self.index_options = options[:options] || {}
        attribute_types = options[:attribute_types] || {}
        options[:attributes].each do |attrib|
          attr_type = attribute_types[attrib] || (self.fields[attrib.to_s] == nil ? nil : self.fields[attrib.to_s].type)
					if attr_type.blank?
						puts "MONGOID SPHINX WARRING: #{attrib} skiped, it need to define type in :attribute_types when it not define in :fields."
						next
					end
          self.search_attributes[attrib] = SPHINX_TYPE_MAPPING[attr_type.to_s] || 'str2ordinal'
        end
        MongoidSphinx.context.add_indexed_model self
      end
			
			def generate_id(object_id)
				#只支持两种类型的主键,且要保证转换后互相不能重复:BSON::ObjectId,Integer.
				#BSON::ObjectId通过crc32转换为数字
				if object_id.is_a?(BSON::ObjectId)
				  id=object_id.to_s.to_crc32
				else
				  raise RuntimeError,"MongoidSphinx require Id must be BSON::ObjectId or Integer" if object_id.is_a?(Integer)==false
				  id=object_id.to_s.to_crc32
				end
				return id
			end

      def internal_sphinx_index
        MongoidSphinx::Index.new(self)
      end

      def has_sphinx_indexes?
        self.search_fields && self.search_fields.length > 0
      end

      def to_riddle
        self.internal_sphinx_index.to_riddle
      end

      def sphinx_stream
        STDOUT.sync = true # Make sure we really stream..

        puts '<?xml version="1.0" encoding="utf-8"?>'
        puts '<sphinx:docset xmlns:sphinx="http://www.redstore.cn/">'

        # Schema
        puts '<sphinx:schema>'
        puts '<sphinx:field name="classname"/>'
        self.search_fields.each do |name|
          puts "<sphinx:field name=\"#{name}\"/>"
        end
				#需要区分主键是否是Integer类型,必须用bigint,因为可能大于32bit.暂时不这样做,因为id类型不一致,会在使用MongoidSphinx.search时发生错误.
				#if self.fields["_id"].type==Integer
				#	puts "<sphinx:attr name=\"_id\" type=\"bigint\" />"
				#else
				#  	puts "<sphinx:attr name=\"_id\" type=\"string\" />"
				#end
				puts "<sphinx:attr name=\"_id\" type=\"string\" />"
				puts "<sphinx:attr name=\"classname\" type=\"string\" />"
        self.search_attributes.each do |key, value|
          puts "<sphinx:attr name=\"#{key}\" type=\"#{value}\" />"
        end
        puts '</sphinx:schema>'

        self.all.each do |document|
          sphinx_compatible_id = self.generate_id(document['_id'])
					puts "<sphinx:document id=\"#{sphinx_compatible_id}\">"

					puts "<classname>#{self.to_s}</classname>"
					puts "<_id>#{document.send("_id")}</_id>"
					self.search_fields.each do |key|
						if document.respond_to?(key.to_sym)
							value = document.send(key)
							value = value.join(",") if value.class == [].class
							puts "<#{key}>#{value.to_s}</#{key}>"
						end
					end
					self.search_attributes.each do |key, value|
						next if self.search_fields.include?(key)
						value = case value
							when 'bool'
								document.send(key) ? 1 : 0
							when 'timestamp'
								document.send(key).to_i
							else
								document.send(key)
						end
						value = value.join(",") if value.class == [].class
						puts "<#{key}>#{value.to_s}</#{key}>"
					end

					puts '</sphinx:document>'
        end

        puts '</sphinx:docset>'
      end
      
      #返回xml
	  #file:写入到文件
      def sphinx_xml(file=false)
	    require 'builder'
		#如果是写入文件中
		if file
		  xml = Builder::XmlMarkup.new(:indent=>2,:target => file)
		else
		  xml = Builder::XmlMarkup.new(:indent=>2)
		end
    	#生成<?xml version="1.0" encoding="UTF-8"?>
		xml.instruct!

		#docset--start
		xml.sphinx(:docset,"xmlns:sphinx"=>"http://www.redstore.cn/") do

		  #schema--start
		  xml.sphinx(:schema) do
			xml.sphinx(:field,"name"=>"classname")
			self.search_fields.each do |name|
			  xml.sphinx(:field,"name"=>name)
			end
			#需要区分主键是否是Integer类型,必须用bigint,因为可能大于32bit.暂时不这样做,因为id类型不一致,会在使用MongoidSphinx.search时发生错误.
			#if self.fields["_id"].type==Integer
			#	xml.sphinx(:attr,"name"=>"_id","type"=>"bigint")
		  	#else
			#	xml.sphinx(:attr,"name"=>"_id","type"=>"string")
		  	#end
			xml.sphinx(:attr,"name"=>"_id","type"=>"string")
			xml.sphinx(:attr,"name"=>"classname","type"=>"string")
        	self.search_attributes.each do |key, value|
			  xml.sphinx(:attr,"name"=>key,"type"=>value)
        	end
		  end#schema--end

		  #document--start
		  self.all.each do |document|
			sphinx_compatible_id = self.generate_id(document['_id'])
			xml.sphinx(:document,"id"=>sphinx_compatible_id) do
			  xml.classname(self.to_s)
			  xml._id(document.send("_id"))
			  self.search_fields.each do |key|
				  if document.respond_to?(key.to_sym)
					  value = document.send(key)
					  value = value.join(",") if value.class == [].class
					  #eval有危险
					  #eval("xml.#{key}('#{value.to_s}')")
					  #xml.method_missing(key,value.to_s)
					  #去掉html标签
					  xml.method_missing(key,value.to_s.gsub(/<\/?[^>]*>/, ""))
				  end
			  end
			  self.search_attributes.each do |key, value|
				  next if self.search_fields.include?(key)
				  value = case value
					  when 'bool'
						  document.send(key) ? 1 : 0
					  when 'timestamp'
						  document.send(key).to_i
					  else
						  document.send(key)
				  end
				  value = value.join(",") if value.class == [].class
				  xml.method_missing(key,value.to_s.gsub(/<\/?[^>]*>/, ""))
			  end
			end
		  end#document--end

		end#docset--end

		#如果是写入文件中
		if file
		  return true
		else
		  return xml.target!
		end
	  end


      def search(query, options = {})
        client = MongoidSphinx::Configuration.instance.client
		
		#修改默认的匹配模式是extended2
        client.match_mode = options[:match_mode] || :extended2
        client.offset = options[:offset].to_i if options.key?(:offset)
        client.limit = options[:limit].to_i if options.key?(:limit)
        client.limit = options[:per_page].to_i if options.key?(:per_page)
        client.offset = (options[:page].to_i - 1) * client.limit if options[:page]
        client.max_matches = options[:max_matches].to_i if options.key?(:max_matches)
        classes = options[:classes] || []
        classes << self
        class_indexes = classes.collect { |klass| "#{klass.to_s.downcase}_core" }.flatten.uniq
        client.set_anchor(*options[:geo_anchor]) if options.key?(:geo_anchor)

        if options.key?(:sort_by)
		  #注意,不是extended2
          client.sort_mode = :extended
          client.sort_by = options[:sort_by]
        end

		#增加字段权重
		#field_weights = {'title' => 10, 'artist' => 10, 'description' => 5, 'search_words'=> 5}
        if options.key?(:field_weights)
          client.field_weights = options[:field_weights]
        end

		#增加评价模式的指定
        if options.key?(:rank_mode)
          client.rank_mode = options[:rank_mode]
        end

        if options.key?(:with)
          options[:with].each do |key, value|
            client.filters << Riddle::Client::Filter.new(key.to_s, value.is_a?(Range) ? value : value.to_a, false)
          end
        end

        if options.key?(:without)
          options[:without].each do |key, value|
            client.filters << Riddle::Client::Filter.new(key.to_s, value.is_a?(Range) ? value : value.to_a, true)
          end
        end
        result = client.query query, class_indexes.join(',')

        MongoidSphinx::Search.new(client, self, result)
      end
    end

  end
end
