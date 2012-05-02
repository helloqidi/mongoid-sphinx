class MongoidSphinx::Context
  attr_reader :indexed_models

  def initialize(*models)
    @indexed_models = []
  end

  def prepare
    MongoidSphinx::Configuration.instance.indexed_models.each do |model|
      add_indexed_model model
    end

    return unless indexed_models.empty?

    load_models
  end

  def define_indexes
    indexed_models.each { |model|
      model.constantize.define_indexes
    }
  end

  def add_indexed_model(model)
    model = model.name if model.is_a?(Class)

    indexed_models << model
    indexed_models.uniq!
    indexed_models.sort!
  end

  private

  # Make sure all models are loaded - without reloading any that
  # ActiveRecord::Base is already aware of (otherwise we start to hit some
  # messy dependencies issues).
  #
  def load_models
    MongoidSphinx::Configuration.instance.model_directories.each do |base|
        begin
          Padrino.dependency_paths.each { |path| Padrino.require_dependencies(path) }
        rescue => err
          STDERR.puts "Warning: Error loading Padrino.dependency_paths"
          STDERR.puts err
        end
    end
  end
end
