Embulk::JavaPlugin.register_input(
  "rethinkdb", "org.embulk.input.rethinkdb.RethinkdbInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
