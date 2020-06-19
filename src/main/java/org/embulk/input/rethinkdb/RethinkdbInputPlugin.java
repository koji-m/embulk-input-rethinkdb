package org.embulk.input.rethinkdb;

import com.google.common.base.Optional;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.ast.ReqlAst;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RethinkdbInputPlugin
        implements InputPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("28015")
        int getPort();

        @Config("database")
        String getDatabase();

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("auth_key")
        @ConfigDefault("null")
        Optional<String> getAuthKey();

        @Config("cert_file")
        @ConfigDefault("null")
        Optional<String> getCertFile();

        @Config("query")
        @ConfigDefault("null")
        Optional<String> getQuery();

        @Config("table")
        @ConfigDefault("null")
        Optional<String> getTable();

        @Config("column_name")
        @ConfigDefault("\"record\"")
        String getColumnName();

        String getReql();
        void setReql(String ast);

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    private static final Logger logger = LoggerFactory.getLogger(RethinkdbInputPlugin.class);

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getAuthKey().isPresent()) {
            throw new ConfigException("auth_key option is not supported yet");
        }

        if (!(task.getUser().isPresent() && task.getPassword().isPresent())) {
            throw new ConfigException("user and password are needed");
        }

        String reql;
        if (task.getQuery().isPresent()) {
            if (task.getTable().isPresent()) {
                throw new ConfigException("only one of 'table' or 'query' parameter is needed");
            }
            reql = String.format("var ast = %s; var res = {ast: ast}; res;", task.getQuery().get());
        }
        else {
            if (!task.getTable().isPresent()) {
                throw new ConfigException("'table' or 'query' parameter is needed");
            }
            reql = String.format("var ast = r.table('%s'); var res = {ast: ast}; res;", task.getTable().get());
        }
        task.setReql(reql);

        Schema schema = Schema.builder().add(task.getColumnName(), Types.JSON).build();
        int taskCount = 1;

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        BufferAllocator allocator = task.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);
        final Column column = pageBuilder.getSchema().getColumns().get(0);

        RethinkDB r = RethinkDB.r;
        ReqlAst ast;
        try {
            ast = compileReQL(r, task.getReql());
        }
        catch (final ScriptException se) {
            throw new ConfigException("ReQL compile error");
        }
        Connection.Builder builder = r.connection()
                .hostname(task.getHost())
                .port(task.getPort())
                .db(task.getDatabase())
                .user(task.getUser().get(), task.getPassword().get());

        Connection conn;
        if (task.getCertFile().isPresent()) {
            Path certFilePath = Paths.get(task.getCertFile().get());
            try (InputStream is = Files.newInputStream(certFilePath)) {
                builder.certFile(is);
                conn = builder.connect();
            } catch (IOException ex) {
                throw new ConfigException("error reading TLS certificate file");
            }
        }
        else {
            conn = builder.connect();
        }

        Cursor cursor = ast.run(conn);
        for (Object doc : cursor) {
            pageBuilder.setJson(column, doc2Value(doc));
            pageBuilder.addRecord();
        }
        conn.close();

        pageBuilder.finish();

        return Exec.newTaskReport();
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }

    private ReqlAst compileReQL(RethinkDB r, String reql) throws ScriptException
    {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");

        engine.put("r", r);

        Bindings bindings = (Bindings) engine.eval(reql);

        return (ReqlAst) bindings.get("ast");
    }

    private Value doc2Value(Object doc) throws DataException
    {
        if (doc == null) {
            return ValueFactory.newNil();
        }
        else if (doc instanceof java.lang.Long) {
            return ValueFactory.newInteger((java.lang.Long) doc);
        }
        else if (doc instanceof java.lang.Double) {
            return ValueFactory.newFloat((java.lang.Double) doc);
        }
        else if (doc instanceof java.lang.String) {
            return ValueFactory.newString((java.lang.String) doc);
        }
        else if (doc instanceof java.lang.Boolean) {
            return ValueFactory.newBoolean((java.lang.Boolean) doc);
        }
        else if (doc instanceof java.time.OffsetDateTime) {
            return ValueFactory.newString(((java.time.OffsetDateTime) doc).toString());
        }
        else if (doc instanceof java.util.List) {
            List<Value> list = new ArrayList<>();
            for (Object obj : ((java.util.List<Object>) doc)) {
                list.add(doc2Value(obj));
            }
            return ValueFactory.newArray(list);
        }
        else if (doc instanceof java.util.Map) {
            Map<Value, Value> map = new HashMap<>();
            Map<Object, Object> m = (Map<Object, Object>) doc;
            Set<Map.Entry<Object, Object>> entries = m.entrySet();
            for (Map.Entry<Object, Object> e : entries) {
                map.put(doc2Value(e.getKey()), doc2Value(e.getValue()));
            }
            return ValueFactory.newMap(map);
        }
        else {
            throw new DataException("Record parse error, unknown document type");
        }
    }
}
