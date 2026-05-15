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
--
-- Per-shape latency capture (V3): every query is timed in pure Lua via
-- ffi clock_gettime(CLOCK_MONOTONIC) -> microsecond precision. Each shape
-- (select_by_id / insert_row / update_by_id / delete_by_id) carries a
-- per-thread cumulative HDR-compatible histogram and a per-thread
-- per-minute interval histogram. The interval histogram is reset every
-- minute on a wall-clock boundary. Final stats lines are emitted in
-- thread_done(); interval lines are emitted from event() when the next
-- minute boundary is crossed.
--
-- Histogram bucket layout matches HdrHistogram_c / HdrHistogram_py exactly
-- (lowest=1 us, highest=60 s, sig_figures=3 -> counts_len=17408, ~0.1%
-- bucket-width error, lossless cross-thread merge by element-wise sum).
-- Sparse non-zero buckets are JSON-encoded inline in each V3 stats line.

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
local op_stats = nil
local query_stats = nil
local interval_op_stats = nil
local interval_query_stats = nil
local query_hist = nil
local interval_query_hist = nil
local thread_start_ms = 0
local interval_start_ms = 0
local next_interval_ms = 60000
local current_minute = 1
local query_templates = {
    select_by_id = {type = "read", category = "select", template = "SELECT c FROM %s WHERE id = %d"},
    insert_row = {type = "write", category = "insert", template = "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')"},
    update_by_id = {type = "write", category = "update", template = "UPDATE %s SET k = k + 1 WHERE id = %d"},
    delete_by_id = {type = "write", category = "delete", template = "DELETE FROM %s WHERE id = %d"},
}
local query_order = {"select_by_id", "insert_row", "update_by_id", "delete_by_id"}
local category_order = {"select", "insert", "update", "delete"}

-- Histogram config: HdrHistogram-compatible bucket layout.
-- Values are recorded in microseconds. lowest=1us, highest=60s, sig_figures=3.
local HDR_LOWEST = 1
local HDR_HIGHEST = 60000000
local HDR_SIG = 3

local function now_ns()
    ffi.C.clock_gettime(CLOCK_MONOTONIC, ts)
    return tonumber(ts[0].tv_sec) * 1000000000 + tonumber(ts[0].tv_nsec)
end

local function now_ms()
    return now_ns() / 1000000
end

-- ---------------------------------------------------------------------------
-- HDR-compatible histogram (pure Lua, mirrors hdr_calculate_bucket_config)
-- ---------------------------------------------------------------------------

local function bit_length(x)
    -- floor(log2(x)) + 1 for positive integers, computed without
    -- math.frexp (which is missing on Lua 5.3+) or LuaJIT 'bit' module
    -- (which is missing on stock PUC Lua). Pure math.floor portable form.
    if x <= 0 then return 0 end
    local n = 0
    local v = x
    while v > 0 do
        v = math.floor(v / 2)
        n = n + 1
    end
    return n
end

local function pow2(n)
    -- Integer power of two, valid up to 2^53 on 64-bit floats.
    return 2 ^ n
end

local function hdr_new()
    local h = {
        lowest = HDR_LOWEST,
        highest = HDR_HIGHEST,
        sig = HDR_SIG,
        total = 0,
        min = nil,
        max = 0,
        counts = {},
    }
    -- bucket layout (mirror of HdrHistogram_c hdr_calculate_bucket_config)
    local largest_value_with_single_unit = 2 * (10 ^ HDR_SIG)
    local sub_bucket_count_magnitude = math.ceil(
        math.log(largest_value_with_single_unit) / math.log(2))
    if sub_bucket_count_magnitude < 1 then sub_bucket_count_magnitude = 1 end
    h.sub_bucket_count_magnitude = sub_bucket_count_magnitude
    h.sub_bucket_half_count_magnitude = sub_bucket_count_magnitude - 1
    if h.sub_bucket_half_count_magnitude < 0 then
        h.sub_bucket_half_count_magnitude = 0
    end
    h.unit_magnitude = math.floor(math.log(HDR_LOWEST) / math.log(2))
    if h.unit_magnitude < 0 then h.unit_magnitude = 0 end
    h.sub_bucket_count = math.floor(pow2(sub_bucket_count_magnitude))
    h.sub_bucket_half_count = math.floor(h.sub_bucket_count / 2)
    h.sub_bucket_mask = (h.sub_bucket_count - 1) * pow2(h.unit_magnitude)
    -- bucket_count: smallest k such that smallest_untrackable > highest
    local smallest_untrackable = h.sub_bucket_count * pow2(h.unit_magnitude)
    local bucket_count = 1
    while smallest_untrackable <= HDR_HIGHEST do
        if smallest_untrackable > 9.0e15 then
            bucket_count = bucket_count + 1
            break
        end
        smallest_untrackable = smallest_untrackable * 2
        bucket_count = bucket_count + 1
    end
    h.bucket_count = bucket_count
    h.counts_len = (bucket_count + 1) * h.sub_bucket_half_count
    return h
end

local function hdr_record(h, value)
    if value < h.lowest then value = h.lowest end
    if value > h.highest then value = h.highest end
    -- bucket_index = bit_length(value | sub_bucket_mask)
    --                - unit_magnitude - sub_bucket_half_count_magnitude - 1
    local masked = value
    if h.sub_bucket_mask > 0 then
        -- Plain Lua doesn't have integer bitwise OR portably. Compute the
        -- equivalent for value, sub_bucket_mask: since sub_bucket_mask is
        -- (2^k - 1) shifted left by unit_magnitude, OR-with-mask = max(value, mask)
        -- when value < mask, otherwise OR doesn't change the high bits being
        -- considered for bit_length(). The algorithm only uses bit_length(),
        -- so masked value's bit_length equals max(bit_length(value),
        -- bit_length(sub_bucket_mask)).
        if value < h.sub_bucket_mask then
            masked = h.sub_bucket_mask
        end
    end
    local pow2ceiling = bit_length(masked)
    local bucket_index = pow2ceiling - h.unit_magnitude
        - h.sub_bucket_half_count_magnitude - 1
    if bucket_index < 0 then bucket_index = 0 end
    -- sub_bucket_index = value >> (bucket_index + unit_magnitude)
    local shift = bucket_index + h.unit_magnitude
    local sub_bucket_index = math.floor(value / pow2(shift))
    -- counts_index = (bucket_index + 1) * sub_bucket_half_count + (sub_bucket_index - sub_bucket_half_count)
    local idx = (bucket_index + 1) * h.sub_bucket_half_count
        + (sub_bucket_index - h.sub_bucket_half_count)
    if idx < 0 then idx = 0 end
    if idx >= h.counts_len then idx = h.counts_len - 1 end
    h.counts[idx] = (h.counts[idx] or 0) + 1
    h.total = h.total + 1
    if h.min == nil or value < h.min then h.min = value end
    if value > h.max then h.max = value end
end

local function hdr_reset(h)
    h.counts = {}
    h.total = 0
    h.min = nil
    h.max = 0
end

local function hdr_serialize(h)
    -- Compact JSON encoding. Field names abbreviated to keep emission bounded:
    --   l  = lowest_trackable_value
    --   h  = highest_trackable_value
    --   s  = significant_figures
    --   t  = total_count
    --   mn = min_value
    --   mx = max_value
    --   b  = sparse non-zero buckets [[idx, count], ...]
    if h.total == 0 then
        return string.format(
            '{"l":%d,"h":%d,"s":%d,"t":0,"mn":0,"mx":0,"b":[]}',
            h.lowest, h.highest, h.sig)
    end
    local pairs_list = {}
    -- Iterate sparse map in sorted index order.
    local idxs = {}
    for k in pairs(h.counts) do
        table.insert(idxs, k)
    end
    table.sort(idxs)
    for _, k in ipairs(idxs) do
        local v = h.counts[k]
        if v and v > 0 then
            table.insert(pairs_list,
                string.format("[%d,%d]", k, v))
        end
    end
    return string.format(
        '{"l":%d,"h":%d,"s":%d,"t":%d,"mn":%d,"mx":%d,"b":[%s]}',
        h.lowest, h.highest, h.sig, h.total,
        h.min or 0, h.max or 0,
        table.concat(pairs_list, ","))
end

-- ---------------------------------------------------------------------------
-- Aggregate stats (count/total/min/avg/max) - retained alongside histograms
-- ---------------------------------------------------------------------------

local function new_stats()
    return {count = 0, total_ms = 0, min_ms = nil, max_ms = 0}
end

local function init_op_stats()
    op_stats = {
        select = new_stats(),
        insert = new_stats(),
        update = new_stats(),
        delete = new_stats(),
    }
    query_stats = {}
    query_hist = {}
    for _, key in ipairs(query_order) do
        query_stats[key] = new_stats()
        query_hist[key] = hdr_new()
    end
    interval_op_stats = {
        select = new_stats(),
        insert = new_stats(),
        update = new_stats(),
        delete = new_stats(),
    }
    interval_query_stats = {}
    interval_query_hist = {}
    for _, key in ipairs(query_order) do
        interval_query_stats[key] = new_stats()
        interval_query_hist[key] = hdr_new()
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
end

local function record_latency(category, query_key, latency_us)
    local latency_ms = latency_us / 1000
    update_stats(op_stats[category], latency_ms)
    update_stats(query_stats[query_key], latency_ms)
    update_stats(interval_op_stats[category], latency_ms)
    update_stats(interval_query_stats[query_key], latency_ms)
    -- Histograms record in microseconds (1 us resolution within the tracked range).
    hdr_record(query_hist[query_key], latency_us)
    hdr_record(interval_query_hist[query_key], latency_us)
end

local function timed_query(category, query_key, sql)
    local start_ns = now_ns()
    con:query(sql)
    local elapsed_us = (now_ns() - start_ns) / 1000
    record_latency(category, query_key, elapsed_us)
end

local function print_stats_line(prefix, tid, stats, extra)
    local avg_ms = 0
    local min_ms = stats.min_ms or 0
    if stats.count > 0 then
        avg_ms = stats.total_ms / stats.count
    end
    print(string.format(
        "%s tid=%d %s count=%d total_ms=%.3f min_ms=%.3f avg_ms=%.3f max_ms=%.3f",
        prefix, tid, extra, stats.count, stats.total_ms, min_ms, avg_ms, stats.max_ms))
end

local function print_query_stats_line(tid, key)
    local spec = query_templates[key]
    local stats = query_stats[key]
    local hist = query_hist[key]
    local avg_ms = 0
    local min_ms = stats.min_ms or 0
    if stats.count > 0 then
        avg_ms = stats.total_ms / stats.count
    end
    print(string.format(
        "CUSTOM_MIXED_QUERY_STATS_V3\ttid=%d\ttype=%s\tcategory=%s\tkey=%s\ttemplate=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f\thist=%s",
        tid, spec.type, spec.category, key, spec.template, stats.count,
        stats.total_ms, min_ms, avg_ms, stats.max_ms,
        hdr_serialize(hist)))
end

local function print_interval_op_stats_line(tid, minute, from_ms, to_ms, category)
    local stats = interval_op_stats[category]
    if stats.count == 0 then
        return
    end
    local avg_ms = stats.total_ms / stats.count
    print(string.format(
        "CUSTOM_MIXED_OP_INTERVAL_V3\ttid=%d\tminute=%d\tfrom_ms=%.0f\tto_ms=%.0f\top=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f",
        tid, minute, from_ms, to_ms, category, stats.count, stats.total_ms,
        stats.min_ms or 0, avg_ms, stats.max_ms))
end

local function print_interval_query_stats_line(tid, minute, from_ms, to_ms, key)
    local stats = interval_query_stats[key]
    local hist = interval_query_hist[key]
    if stats.count == 0 then
        return
    end
    local spec = query_templates[key]
    local avg_ms = stats.total_ms / stats.count
    print(string.format(
        "CUSTOM_MIXED_QUERY_INTERVAL_V3\ttid=%d\tminute=%d\tfrom_ms=%.0f\tto_ms=%.0f\ttype=%s\tcategory=%s\tkey=%s\ttemplate=%s\tcount=%d\ttotal_ms=%.3f\tmin_ms=%.3f\tavg_ms=%.3f\tmax_ms=%.3f\thist=%s",
        tid, minute, from_ms, to_ms, spec.type, spec.category, key,
        spec.template, stats.count, stats.total_ms, stats.min_ms or 0,
        avg_ms, stats.max_ms,
        hdr_serialize(hist)))
end

local function reset_interval_stats()
    for _, category in ipairs(category_order) do
        interval_op_stats[category] = new_stats()
    end
    for _, key in ipairs(query_order) do
        interval_query_stats[key] = new_stats()
        hdr_reset(interval_query_hist[key])
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
        timed_query("delete", "delete_by_id",
            string.format(query_templates.delete_by_id.template, table_name, id))
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
