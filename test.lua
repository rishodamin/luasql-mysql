local mysql = require("mysql")
local env = mysql.mysql()
local conn = env:connect("kct", "root", "Rishofencing123", "localhost", 3306)

if conn then
    print("Connected to MySQL!")
   -- print(conn.prepare)

    -- local stmt = conn:prepare("INSERT INTO student (id, name, major, cgpa) VALUES (?, ?, ?, ?)")
    -- stmt:bind(1, 16)
    -- stmt:bind(2, "new")
    -- stmt:bind(3, "EEE")
    -- stmt:bind(4, 8)

    -- local stmt = conn:prepare("delete from student where id > ?")
    -- stmt:bind(1, 9)

   -- local stmt = conn:prepare("delete from student where id = 8")
    local stmt = conn:prepare("select * from student")
    local cursor = stmt:execute()
    local fields = cursor:fields()
    for key, value in pairs(fields) do
        print(key .. ": " .. value)
    end
    print()
    local row = cursor:fetch("uun") -- Fetch rows as a table with column names as keys
    while row do
        for key, value in pairs(row) do
            print(key .. ": " .. tostring(value))
        end
        print("--------------------")
        row = cursor:fetch("jbnn")
    end
    stmt:finalize()
    conn:close()
else
    print("Connection failed.")
end

env:close()