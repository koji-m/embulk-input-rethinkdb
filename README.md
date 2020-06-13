# Rethinkdb input plugin for Embulk

RethinkDB input plugin for Embulk loads records from RethinkDB.

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **host**: database host name (string, required)
- **port**: database port number (integer, default: `28015`)
- **database**: database name (string, required)
- **user**: database login user name (string, required)
- **password**: database login password (string, required)
- **query**: ReQL to run (string, required)
- **column_name**: column name used in outputs (string, default: `"record"`)

## ReQL for query option

ReQL specified in query option is described according to the following syntax rules.

- The syntax is JavaScript (ECMAScript 5.1) runs on Nashorn (JavaScript Engine developed in Java).
- The code must be an expression that evaluates to ReqlAst. (No trailing comma required)
- The RethinkDB object can be referenced as the variable `r`.

## Example

```yaml
in:
  type: rethinkdb
  host: 'rethink'
  port: 28015
  database: 'test'
  user: 'admin'
  password: ''
  query:
    r.table("authors").filter(function(row) {
      return row.g("tv_show").eq("Battlestar Galactica");
    })
out: {type: stdout}
```


## Build

```
$ ./gradlew gem
```
