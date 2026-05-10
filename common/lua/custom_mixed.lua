-- Mixed read+IUD workload matching production ratios:
--   ~88% reads, ~8% inserts, ~3% updates, ~1% deletes
-- (derived from: 38.3K reads + 3.6K inserts + 1.5K updates + 0.2K deletes = 43.6K ops/sec)
--
-- Self-contained: defines its own prepare()/cleanup() so it works on DSQL,
-- which rejects SERIAL/AUTO_INCREMENT and synchronous CREATE INDEX.
-- Schema mirrors stock sysbench (id, k, c, pad) but uses GENERATED ALWAYS
-- AS IDENTITY (pgsql/DSQL) or AUTO_INCREMENT (mysql) for id, and OMITS the
-- secondary index on k -- the event() workload only queries by primary key
-- (WHERE id = X), so the index would burn CREATE INDEX ASYNC quota for no
-- runtime benefit. event()'s INSERTs continue past the prepare-loaded
-- range; SELECT/UPDATE/DELETE target [1, table_size] which matches the
-- contiguous identity range emitted by prepare.

sysbench.cmdline.options = {
    tables = {"Number of tables", 32},
    table_size = {"Number of rows per table", 1000000},
}

local ffi = require("ffi")

ffi.cdef[[
struct timespec {
    long tv_sec;
    long tv_nsec;
};
int clock_gettime(int clk_id, struct timespec *tp);
]]

local CLOCK_MONOTONIC = 1
local ts = ffi.new("struct timespec[1]")
local latency_buckets_ms = {0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192}
local op_stats = nil
local query_stats = nil
local interval_op_stats = nil
local interval_query_stats = nil
local thread_start_ms = 0
local interval_start_ms = 0
local next_interval_ms = 60000
local current_minute = 1
local query_templates = {
    select_by_id = {type = "read", category = "select", template = "SELECT c FROM %s WHERE id = %d"},
    insert_row = {type = "write", category = "insert", template = "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')"},
    update_by_id = {type = "write", category = "update", template = "UPDATE %s SET k = k + 1 WHERE id = %d"},
    delete_by_id = {type = "write", category = "delete", template = "DELETE FROM %s WHERE id = %d"},
    delete_by_id_limit = {type = "write", category = "delete", template = "DELETE FROM %s WHERE id = %d LIMIT 1"},
}
local query_order = {"select_by_id", "insert_row", "update_by_id", "delete_by_id", "delete_by_id_limit"}
local category_order = {"select", "insert", "update", "delete"}

local function now_ms()
    ffi.C.clock_gettime(CLOCK_MONOTONIC, ts)
    return tonumber(ts[0].tv_sec) * 1000 + tonumber(ts[0].tv_nsec) / 1000000
end

local function new_stats()
    local buckets = {}
    for i = 1, #latency_buckets_ms do
        buckets[i] = 0
    end
    return {count = 0, total_ms = 0, min_ms = nil, max_ms = 0, buckets = buckets}
end

local function init_op_stats()
    op_stats = {
        select = new_stats(),
        insert = new_stats(),
        update = new_stats(),
        delete = new_stats(),
    }
    query_stats = {}
    for _, key in ipairs(query_order) do
        query_stats[key] = new_stats()
    end
    interval_op_stats = {
        select = new_stats(),
        insert = new_stats(),
        update = new_stats(),
        delete = new_stats(),
    }
    interval_query_stats = {}
    for _, key in ipairs(query_order) do
        interval_query_stats[key] = new_stats()
    end
    thread_start_ms = now_ms()
    interval_start_ms = 0
    next_interval_ms = 60000
    current_minute = 1
end

local function update_stats(stats, latency_ms)
    stats.count = stats.count + 1
    stats.total_ms = stats.total_ms + latency_ms
    if stats.min_ms == nil or latency_ms < stats.min_ms then
        stats.min_ms = latency_ms
    end
    if latency_ms > stats.max_ms then
        stats.max_ms = latency_ms
    end
    for i = 1, #latency_buckets_ms do
        if latency_ms <= latency_buckets_ms[i] then
            stats.buckets[i] = stats.buckets[i] + 1
            return
        end
    end
    stats.buckets[#stats.buckets] = stats.buckets[#stats.buckets] + 1
end

local function record_latency(category, query_key, latency_ms)
    update_stats(op_stats[category], latency_ms)
    update_stats(query_stats[query_key], latency_ms)
    update_stats(interval_op_stats[category], latency_ms)
    update_stats(interval_query_stats[query_key], latency_ms)
end

local function timed_query(category, query_key, sql)
    local start_ms = now_ms()
    con:query(sql)
    record_latency(category, query_key, now_ms() - start_ms)
end

local function print_stats_line(prefix, tid, stats, extra)
    local avg_ms = 0
    local min_ms = stats.min_ms or 0
    if stats.count > 0 then
        avg_ms = stats.total_ms / stats.count
    end
    print(string.format(
        "%s tid=%d %s count=%d total_ms=%.3f min_ms=%.3f avg_ms=%.3f max_ms=%.3f buckets=%s",
        prefix, tid, extra, stats.count, stats.total_ms, min_ms, avg_ms, stats.max_ms,
        table.concat(stats.buckets, ",")))
end

local function print_query_stats_line(tid, key)
    local spec = query_templates[key]
    local stats = query_stats[key]
    local avg_ms = 0
    local min_ms = stats.min_ms or 0
    if stats.count > 0 then
        avg_ms = stats.total_ms / stats.count
    end
    print(string.format(
        "CUSTOM_MIXED_QUERY_STATS_V1\ttid=%d\ttype=%s\tcategory=%s\tkey=%s\ttemplate=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f\tbuckets=%s",
        tid, spec.type, spec.category, key, spec.template, stats.count,
        stats.total_ms, min_ms, avg_ms, stats.max_ms,
        table.concat(stats.buckets, ",")))
end

local function print_interval_op_stats_line(tid, minute, from_ms, to_ms, category)
    local stats = interval_op_stats[category]
    if stats.count == 0 then
        return
    end
    local avg_ms = stats.total_ms / stats.count
    print(string.format(
        "CUSTOM_MIXED_OP_INTERVAL_V1\ttid=%d\tminute=%d\tfrom_ms=%.0f\tto_ms=%.0f\top=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f\tbuckets=%s",
        tid, minute, from_ms, to_ms, category, stats.count, stats.total_ms,
        stats.min_ms or 0, avg_ms, stats.max_ms, table.concat(stats.buckets, ",")))
end

local function print_interval_query_stats_line(tid, minute, from_ms, to_ms, key)
    local stats = interval_query_stats[key]
    if stats.count == 0 then
        return
    end
    local spec = query_templates[key]
    local avg_ms = stats.total_ms / stats.count
    print(string.format(
        "CUSTOM_MIXED_QUERY_INTERVAL_V1\ttid=%d\tminute=%d\tfrom_ms=%.0f\tto_ms=%.0f\ttype=%s\tcategory=%s\tkey=%s\ttemplate=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f\tbuckets=%s",
        tid, minute, from_ms, to_ms, spec.type, spec.category, key,
        spec.template, stats.count, stats.total_ms, stats.min_ms or 0,
        avg_ms, stats.max_ms, table.concat(stats.buckets, ",")))
end

local function reset_interval_stats()
    for _, category in ipairs(category_order) do
        interval_op_stats[category] = new_stats()
    end
    for _, key in ipairs(query_order) do
        interval_query_stats[key] = new_stats()
    end
end

local function print_interval_stats(minute, from_ms, to_ms)
    local tid = sysbench.tid or -1
    for _, category in ipairs(category_order) do
        print_interval_op_stats_line(tid, minute, from_ms, to_ms, category)
    end
    for _, key in ipairs(query_order) do
        print_interval_query_stats_line(tid, minute, from_ms, to_ms, key)
    end
end

local function interval_has_data()
    for _, category in ipairs(category_order) do
        if interval_op_stats[category].count > 0 then
            return true
        end
    end
    return false
end

local function maybe_print_interval_stats()
    local elapsed_ms = now_ms() - thread_start_ms
    while elapsed_ms >= next_interval_ms do
        if interval_has_data() then
            print_interval_stats(current_minute, interval_start_ms, next_interval_ms)
        end
        reset_interval_stats()
        interval_start_ms = next_interval_ms
        next_interval_ms = next_interval_ms + 60000
        current_minute = current_minute + 1
    end
end

local function print_op_stats()
    local tid = sysbench.tid or -1
    for _, category in ipairs(category_order) do
        print_stats_line("CUSTOM_MIXED_OP_STATS", tid, op_stats[category], "op=" .. category)
    end
    for _, key in ipairs(query_order) do
        print_query_stats_line(tid, key)
    end
end

function thread_init()
    init_op_stats()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    local elapsed_ms = now_ms() - thread_start_ms
    if interval_has_data() then
        print_interval_stats(current_minute, interval_start_ms, elapsed_ms)
    end
    print_op_stats()
    con:disconnect()
end

function event()
    local num_tables = tonumber(sysbench.opt.tables) or 32
    local table_size = tonumber(sysbench.opt.table_size) or 1000000
    local table_name = "sbtest" .. sysbench.rand.uniform(1, num_tables)
    local id = sysbench.rand.uniform(1, table_size)
    local r = sysbench.rand.uniform(1, 1000)

    if r <= 878 then
        timed_query("select", "select_by_id",
            string.format(query_templates.select_by_id.template, table_name, id))
    elseif r <= 961 then
        local k_val = sysbench.rand.uniform(1, table_size)
        local c_val = sysbench.rand.string(string.rep("@", 120))
        local pad_val = sysbench.rand.string(string.rep("@", 60))
        timed_query("insert", "insert_row",
            string.format(query_templates.insert_row.template, table_name, k_val, c_val, pad_val))
    elseif r <= 995 then
        timed_query("update", "update_by_id",
            string.format(query_templates.update_by_id.template, table_name, id))
    else
        local driver = drv:name()
        if driver == "pgsql" then
            timed_query("delete", "delete_by_id",
                string.format(query_templates.delete_by_id.template, table_name, id))
        else
            timed_query("delete", "delete_by_id_limit",
                string.format(query_templates.delete_by_id_limit.template, table_name, id))
        end
    end

    maybe_print_interval_stats()
end

function prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    local driver_name = drv:name()
    local num_tables = sysbench.opt.tables
    local table_size = sysbench.opt.table_size
    local num_threads = sysbench.opt.threads or 1
    local tid = sysbench.tid or 0

    for i = tid + 1, num_tables, num_threads do
        local tbl = "sbtest" .. i

        print(string.format("[tid=%d] Creating table %s...", tid, tbl))
        con:query(string.format("DROP TABLE IF EXISTS %s", tbl))

        if driver_name == "pgsql" then
            con:query(string.format([[
                CREATE TABLE %s (
                    -- DSQL requires explicit CACHE >= 65536 (or = 1) on IDENTITY columns;
                    -- 65536 minimizes round-trips during prepare's bulk INSERTs.
                    id BIGINT GENERATED ALWAYS AS IDENTITY (CACHE 65536) PRIMARY KEY,
                    k INTEGER NOT NULL DEFAULT 0,
                    c CHAR(120) NOT NULL DEFAULT '',
                    pad CHAR(60) NOT NULL DEFAULT ''
                )
            ]], tbl))
        else
            con:query(string.format([[
                CREATE TABLE %s (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    k INTEGER NOT NULL DEFAULT 0,
                    c CHAR(120) NOT NULL DEFAULT '',
                    pad CHAR(60) NOT NULL DEFAULT ''
                )
            ]], tbl))
        end

        local batch_size = 500
        local rows_inserted = 0

        while rows_inserted < table_size do
            local remaining = table_size - rows_inserted
            local current_batch = math.min(batch_size, remaining)

            local parts = {}
            for j = 1, current_batch do
                local k_val = sysbench.rand.uniform(1, table_size)
                local c_val = sysbench.rand.string(string.rep("@", 120))
                local pad_val = sysbench.rand.string(string.rep("@", 60))
                table.insert(parts, string.format(
                    "(%d, '%s', '%s')",
                    k_val, c_val, pad_val))
            end

            if #parts > 0 then
                con:query(string.format(
                    "INSERT INTO %s (k, c, pad) VALUES %s",
                    tbl, table.concat(parts, ",")))
            end

            rows_inserted = rows_inserted + current_batch
            if rows_inserted % 100000 == 0 then
                print(string.format("  [tid=%d] %s: %d / %d rows", tid, tbl, rows_inserted, table_size))
            end
        end

        print(string.format("[tid=%d] %s: %d rows inserted", tid, tbl, table_size))
    end

    con:disconnect()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    local num_tables = sysbench.opt.tables
    local num_threads = sysbench.opt.threads or 1
    local tid = sysbench.tid or 0

    for i = tid + 1, num_tables, num_threads do
        print(string.format("[tid=%d] Dropping table sbtest%d...", tid, i))
        con:query(string.format("DROP TABLE IF EXISTS sbtest%d", i))
    end

    con:disconnect()
end
