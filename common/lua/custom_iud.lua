-- Self-contained IUD workload (no oltp_common dependency)
--
-- Ratio: 68% INSERT, 28% UPDATE, 4% DELETE
-- Derived from production Aurora MySQL baseline metrics.

sysbench.cmdline.options = {
    tables = {"Number of tables", 8},
    table_size = {"Number of rows per table", 100000},
}

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local num_tables = tonumber(sysbench.opt.tables) or 8
    local table_size = tonumber(sysbench.opt.table_size) or 100000
    local table_name = "sbtest" .. sysbench.rand.uniform(1, num_tables)
    local id = sysbench.rand.uniform(1, table_size)
    local k_val = sysbench.rand.uniform(1, table_size)
    local c_val = sysbench.rand.string(string.rep("@", 120))
    local pad_val = sysbench.rand.string(string.rep("@", 60))
    local r = sysbench.rand.uniform(1, 100)

    con:query("BEGIN")

    if r <= 68 then
        con:query(string.format(
            "INSERT INTO %s (k, c, pad) VALUES (%d, '%s', '%s')",
            table_name, k_val, c_val, pad_val))
    elseif r <= 96 then
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
