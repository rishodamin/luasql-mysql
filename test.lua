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

    local stmt = conn:prepare("delete from student where id = 8")
   -- local stmt = conn:prepare("select * from student")

    stmt:execute()
    stmt:finalize()
    conn:close()
else
    print("Connection failed.")
end

env:close()