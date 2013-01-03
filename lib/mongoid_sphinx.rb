require "mongoid"
require "riddle"
require "riddle/1.10"

require 'mongoid_sphinx/configuration'
require 'mongoid_sphinx/context'
require 'mongoid_sphinx/index'
require 'mongoid_sphinx/search'
require 'mongoid_sphinx/mongoid/identity'
require 'mongoid_sphinx/mongoid/sphinx'
require 'mongoid_sphinx/core/string'
#require 'mongoid_sphinx/railtie' if defined?(Rails)

module MongoidSphinx

  @@sphinx_mutex = Mutex.new
  @@context      = nil

  def self.context
    if @@context.nil?
      @@sphinx_mutex.synchronize do
        if @@context.nil?
          @@context = MongoidSphinx::Context.new
          @@context.prepare
        end
      end
    end

    @@context
  end

  def self.reset_context!
    @@sphinx_mutex.synchronize do
      @@context = nil
    end
  end

  def self.pid_active?(pid)
    !!Process.kill(0, pid.to_i)
  rescue Errno::EPERM => e
    true
  rescue Exception => e
    false
  end

  def self.sphinx_running?
    !!sphinx_pid && pid_active?(sphinx_pid)
  end

  def self.sphinx_pid
    if File.exists?(MongoidSphinx::Configuration.instance.pid_file)
      File.read(MongoidSphinx::Configuration.instance.pid_file)[/\d+/]
    else
      nil
    end
  end

end
