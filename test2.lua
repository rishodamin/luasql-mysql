local mysql = require("mysql")
local env = mysql.mysql()
local conn = env:connect("kct", "root", "Rishofencing123", "localhost", 3306)

if not conn then
    print("Failed to connect to the database.")
    return
end

-- Execute a SELECT query
local cursor, errorMessage = conn:execute("SELECT * FROM student")

if not cursor then
    print("Query execution failed: " .. errorMessage)
    conn:close()
    env:close()
    return
end

-- Fetch and print results
local row = cursor:fetch({}, "j") -- Fetch rows as a table with column names as keys
while row do
    for key, value in pairs(row) do
        print(key .. ": " .. tostring(value) .. '\n')
    end
    print("--------------------")
    row = cursor:fetch({}, "j")
end

-- Cleanup
cursor:close()
conn:close()
env:close()
