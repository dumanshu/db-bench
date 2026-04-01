-- KV-over-SQL benchmark: two profiles via 'kv_profile' option:
--   ml      - 90% read (84% point, 3% prefix, 3% range, 1% scan), 6% conditional upsert, 1% delete
--   legacy  - 97% point reads, 2% unconditional upsert, 1% delete

sysbench.cmdline.options = {
   tables = {"Number of tables", 16},
   table_size = {"Number of rows per table", 100000},
   kv_profile = {"Workload profile: ml or legacy", "ml"},
   pk_size = {"Primary key size in bytes", 32},
   sk_size = {"Secondary key size in bytes", 64},
   value_size_read = {"Median read value size in bytes", 810},
   value_size_write_ml = {"Median write value size for ml profile in bytes", 607},
   value_size_write_legacy = {"Median write value size for legacy profile in bytes", 751},
   max_versions = {"Max versions per key before cleanup", 3},
   prefix_limit = {"LIMIT for prefix queries", 10},
   range_limit = {"LIMIT for range queries", 20},
   scan_limit = {"LIMIT for full scan pagination", 100},
}

local con
local drv
local profile
local num_tables
local table_size
local pk_size
local sk_size
local val_size_write
local max_versions

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   profile = sysbench.opt.kv_profile
   num_tables = sysbench.opt.tables
   table_size = sysbench.opt.table_size
   pk_size = sysbench.opt.pk_size
   sk_size = sysbench.opt.sk_size
   max_versions = sysbench.opt.max_versions

   if profile == "legacy" then
      val_size_write = sysbench.opt.value_size_write_legacy
   else
      val_size_write = sysbench.opt.value_size_write_ml
   end
end

function thread_done()
   con:disconnect()
end

local function random_table()
   return "kv_data_" .. sysbench.rand.uniform(1, num_tables)
end

local function random_pk()
   return sysbench.rand.string(string.rep("@", pk_size))
end

local function random_sk()
   return sysbench.rand.string(string.rep("@", sk_size))
end

local function random_value(size)
   return sysbench.rand.string(string.rep("@", size))
end

local function random_timestamp()
   return sysbench.rand.uniform(1, 2000000000)
end

local function existing_key_id()
   return sysbench.rand.uniform(1, table_size)
end

local function point_query()
   local tbl = random_table()
   local key_id = existing_key_id()
   con:query(string.format(
      "SELECT * FROM %s WHERE primary_key = '%s' AND secondary_key = '%s' ORDER BY timestamp DESC LIMIT 1",
      tbl,
      string.format("pk_%010d", key_id),
      string.format("sk_%010d", key_id)
   ))
end

local function prefix_query()
   local tbl = random_table()
   local key_id = existing_key_id()
   local prefix = string.format("sk_%05d", key_id % 100000)
   con:query(string.format(
      "SELECT * FROM %s WHERE primary_key = '%s' AND secondary_key LIKE concat(cast('%s' as char), '%%') ORDER BY secondary_key ASC LIMIT %d",
      tbl,
      string.format("pk_%010d", key_id),
      prefix,
      sysbench.opt.prefix_limit
   ))
end

local function range_query()
   local tbl = random_table()
   local key_id = existing_key_id()
   local sk_start = string.format("sk_%010d", key_id)
   local sk_end = string.format("sk_%010d", key_id + 100)
   con:query(string.format(
      "SELECT * FROM %s WHERE primary_key = '%s' AND secondary_key >= '%s' AND secondary_key <= '%s' ORDER BY secondary_key ASC LIMIT %d",
      tbl,
      string.format("pk_%010d", key_id),
      sk_start,
      sk_end,
      sysbench.opt.range_limit
   ))
end

local function full_scan()
   local tbl = random_table()
   local key_id = existing_key_id()
   con:query(string.format(
      "SELECT * FROM %s WHERE (primary_key > '%s') OR (primary_key = '%s' AND secondary_key > '%s') ORDER BY primary_key, secondary_key LIMIT %d",
      tbl,
      string.format("pk_%010d", key_id),
      string.format("pk_%010d", key_id),
      string.format("sk_%010d", key_id),
      sysbench.opt.scan_limit
   ))
end

local function upsert_conditional()
   local tbl = random_table()
   local key_id = existing_key_id()
   local pk = string.format("pk_%010d", key_id)
   local sk = string.format("sk_%010d", key_id)
   local ts = random_timestamp()
   local val = random_value(val_size_write)

   con:query("BEGIN")
   con:query(string.format(
      "INSERT INTO %s (primary_key, secondary_key, timestamp, value) VALUES ('%s', '%s', %d, '%s') " ..
      "ON DUPLICATE KEY UPDATE " ..
      "timestamp = IF(VALUES(timestamp) >= timestamp, VALUES(timestamp), timestamp), " ..
      "value = IF(VALUES(timestamp) >= timestamp, VALUES(value), value)",
      tbl, pk, sk, ts, val
   ))
   con:query("COMMIT")
end

local function upsert_unconditional()
   local tbl = random_table()
   local key_id = existing_key_id()
   local pk = string.format("pk_%010d", key_id)
   local sk = string.format("sk_%010d", key_id)
   local ts = random_timestamp()
   local val = random_value(val_size_write)

   con:query("BEGIN")
   con:query(string.format(
      "INSERT INTO %s (primary_key, secondary_key, timestamp, value) VALUES ('%s', '%s', %d, '%s') " ..
      "ON DUPLICATE KEY UPDATE value = VALUES(value)",
      tbl, pk, sk, ts, val
   ))
   con:query("COMMIT")
end

local function version_delete()
   local tbl = random_table()
   local key_id = existing_key_id()
   local pk = string.format("pk_%010d", key_id)
   local sk = string.format("sk_%010d", key_id)

   con:query("BEGIN")
   con:query(string.format(
      "DELETE FROM %s WHERE primary_key = '%s' AND secondary_key = '%s' " ..
      "AND timestamp < (" ..
         "SELECT ts FROM (SELECT timestamp AS ts FROM %s WHERE primary_key = '%s' AND secondary_key = '%s' " ..
         "ORDER BY timestamp DESC LIMIT 1 OFFSET %d) AS subq" ..
      ")",
      tbl, pk, sk,
      tbl, pk, sk,
      max_versions
   ))
   con:query("COMMIT")
end

function event()
   local r = sysbench.rand.uniform(1, 1000)

   if profile == "ml" then
-- 84% point, 3% prefix, 3% range, 1% scan, 6% upsert, 1% delete
if r <= 840 then
point_query()
elseif r <= 870 then
prefix_query()
elseif r <= 900 then
range_query()
elseif r <= 910 then
full_scan()
elseif r <= 970 then
upsert_conditional()

      else
         version_delete()
      end
   else
      -- 97% point, 2% upsert, 1% delete
      if r <= 970 then
         point_query()
      elseif r <= 990 then
         upsert_unconditional()
      else
         version_delete()
      end
   end
end

function prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   local num_tables = sysbench.opt.tables
   local table_size = sysbench.opt.table_size
   local pk_sz = sysbench.opt.pk_size
   local sk_sz = sysbench.opt.sk_size
   local val_sz = sysbench.opt.value_size_read

   for i = 1, num_tables do
      local tbl = "kv_data_" .. i

      print(string.format("Creating table %s...", tbl))
      con:query(string.format("DROP TABLE IF EXISTS %s", tbl))
      con:query(string.format([[
         CREATE TABLE %s (
            primary_key VARBINARY(1024) NOT NULL,
            secondary_key VARBINARY(1024) NOT NULL,
            timestamp BIGINT NOT NULL,
            value MEDIUMBLOB,
            PRIMARY KEY (primary_key, secondary_key, timestamp) CLUSTERED
         )
      ]], tbl))

      local batch_size = 1000
      local rows_inserted = 0

      while rows_inserted < table_size do
         local remaining = table_size - rows_inserted
         local current_batch = math.min(batch_size, remaining)

         local parts = {}
         for j = 1, current_batch do
            local row_id = rows_inserted + j
            local pk = string.format("pk_%010d", row_id)
            local sk = string.format("sk_%010d", row_id)
            for v = 1, sysbench.opt.max_versions do
               table.insert(parts, string.format(
                  "('%s', '%s', %d, '%s')",
                  pk, sk, 1000000000 + v,
                  sysbench.rand.string(string.rep("@", val_sz))
               ))
            end
         end

         if #parts > 0 then
            con:query(string.format(
               "INSERT INTO %s (primary_key, secondary_key, timestamp, value) VALUES %s",
               tbl, table.concat(parts, ",")
            ))
         end

         rows_inserted = rows_inserted + current_batch
         if rows_inserted % 10000 == 0 then
            print(string.format("  %s: %d / %d rows", tbl, rows_inserted, table_size))
         end
      end

      print(string.format("  %s: %d rows inserted (%d versions each)", tbl, table_size, sysbench.opt.max_versions))
   end

   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()
   local num_tables = sysbench.opt.tables

   for i = 1, num_tables do
      print(string.format("Dropping table kv_data_%d...", i))
      con:query(string.format("DROP TABLE IF EXISTS kv_data_%d", i))
   end

   con:disconnect()
end
