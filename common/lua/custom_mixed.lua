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

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local num_tables = tonumber(sysbench.opt.tables) or 32
    local table_size = tonumber(sysbench.opt.table_size) or 1000000
    local table_name = "sbtest" .. sysbench.rand.uniform(1, num_tables)
    local id = sysbench.rand.uniform(1, table_size)
    local r = sysbench.rand.uniform(1, 1000)

    con:query("BEGIN")

    if r <= 878 then
        con:query(string.format(
            "SELECT c FROM %s WHERE id = %d",
            table_name, id))
    elseif r <= 961 then
        local k_val = sysbench.rand.uniform(1, table_size)
        local c_val = sysbench.rand.string(string.rep("@", 120))
        local pad_val = sysbench.rand.string(string.rep("@", 60))
        con:query(string.format(
            "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')",
            table_name, k_val, c_val, pad_val))
    elseif r <= 995 then
        con:query(string.format(
            "UPDATE %s SET k = k + 1 WHERE id = %d",
            table_name, id))
    else
        local driver = drv:name()
        if driver == "pgsql" then
            con:query(string.format(
                "DELETE FROM %s WHERE id = %d",
                table_name, id))
        else
            con:query(string.format(
                "DELETE FROM %s WHERE id = %d LIMIT 1",
                table_name, id))
        end
    end

    con:query("COMMIT")
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
