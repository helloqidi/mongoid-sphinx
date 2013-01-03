# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)

require "mongoid_sphinx/version"

Gem::Specification.new do |s|
  s.name        = "mongoid-sphinx-helloqidi"
  s.version     = MongoidSphinx::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Matt Hodgson"]
  s.email       = ["mhodgson@redbeard-tech.com"]
  s.homepage    = "http://github.com/helloqidi/mongoid-sphinx"
  s.summary     = "A full text indexing extension for MongoDB using Sphinx and Mongoid."
  s.description = "A full text indexing extension for MongoDB using Sphinx and Mongoid."

  s.required_rubygems_version = ">= 1.3.6"

  s.add_dependency("mongoid", [">= 2.0.0"])
  s.add_dependency("riddle", ["~> 1.2.2"])

  s.files        = Dir.glob("lib/**/*") + %w(README.markdown)
  s.require_path = 'lib'
end
