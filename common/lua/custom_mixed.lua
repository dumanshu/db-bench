-- Mixed read+IUD workload matching production ratios:
--   ~88% reads, ~8% inserts, ~3% updates, ~1% deletes
-- (derived from: 38.3K reads + 3.6K inserts + 1.5K updates + 0.2K deletes = 43.6K ops/sec)

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
        con:query(string.format(
            "DELETE FROM %s WHERE id = %d LIMIT 1",
            table_name, id))
    end

    con:query("COMMIT")
end
