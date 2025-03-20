local mysql = require("luasql.mysql")
local env = mysql.mysql()
local conn = env:connect("kct", "root", "Rishofencing123", "localhost", 3306)

if conn then
    print("Connected to MySQL!")
    conn:close()
else
    print("Connection failed.")
end

env:close()