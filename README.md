# LuaSQL MySQL - Prepared Statement Support (Development Purpose)

This repository provides **prepared statements** support in LuaSQL for MySQL for development purposes. This base implementation will be used in LuaSQL for GSOC 25 and will be extended if my proposal gets selected. Features to be implemented next include bulk insert from tables and proper error handling.

## Prerequisites

### 1. Install Lua and MySQL Dependencies
```sh
sudo apt install lua5.3 liblua5.3-0 liblua5.3-dev
```

### 2. Install MySQL Server & Client
```sh
sudo apt install mysql-server mysql-client -y
```

### 3. Install MySQL Development Libraries
```sh
sudo apt install libmysqlclient-dev
```

### 4. Start & Enable MySQL
```sh
sudo systemctl start mysql
sudo systemctl enable mysql
```

### 5. Set Password to Work Remotely
```sh
sudo mysql
```

Then, execute the following commands inside MySQL:
```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'your_password';
FLUSH PRIVILEGES;
EXIT;
```

### 6. Create Database, Table, and Insert Records
```sh
mysql -u root -p
```
Enter the password you set and run:
```sql
CREATE DATABASE school;
USE school;
CREATE TABLE student (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    cgpa DECIMAL(3,2) NULL
);
INSERT INTO student (name, cgpa) VALUES
('Alice', 9.2),
('Bob', 8.5),
('Charlie', NULL),
('David', 8.9);
SELECT * FROM student;
EXIT;
```

### 7. Clone This Repository & Test Prepared Statements
```sh
git clone https://github.com/rishodamin/luasql-mysql.git
cd luasql-mysql
```

After cloning the repository, create a new Lua file named `main.lua` in the same directory and add the following example code.

## Usage

### Creating `main.lua`
Create a new file named `main.lua` and paste the following code inside it:

### Connecting to MySQL
```lua
local mysql = require("mysql") -- MySQL wrapper for testing purposes
local env = mysql.mysql()
local conn = env:connect("school", "root", "your_password", "localhost", 3306)

if conn then
    print("Connected to MySQL!")
```

### Preparing and Executing a Statement
```lua
    local stmt = conn:prepare("SELECT * FROM student WHERE id > ?")
    stmt:bind(1, 2) -- Bind value 2 to the first '?' occurrence
    local cursor = stmt:execute()
    
    local fields = cursor:fields() -- Fetch field names if needed
    for key, value in pairs(fields) do
        print(key .. ": " .. value)
    end
    print()
    
    local row = cursor:fetch("a") -- Fetch rows as a table with column names as keys
    while row do
        for key, value in pairs(row) do
            print(key .. ": " .. tostring(value))
        end
        print("--------------------")
        row = cursor:fetch("a")
    end
    
    stmt:finalize()
    conn:close()
else
    print("Connection failed.")
end

env:close()
```

### Running the Script
Once you have created `main.lua`, run the script using:
```sh
lua main.lua
```

### Fetching Field Names Before Results
```lua
local fields = cursor:fields() -- Returns a key-value table like:
-- 1: id
-- 2: name
-- 3: cgpa
```

### Inserting Data Using Prepared Statements
```lua
local stmt = conn:prepare("INSERT INTO student (id, name, cgpa) VALUES (?, ?, ?)")
stmt:bind(1, 8)
stmt:bind(2, "new")
stmt:bind(3, 8.5)
local rows_affected = stmt:execute() -- Returns affected rows for non-SELECT queries
```

### Deleting Data Using Prepared Statements
```lua
local stmt = conn:prepare("DELETE FROM student WHERE id > ?")
stmt:bind(1, 9)
local rows_affected = stmt:execute()
```

## Future Enhancements
- **Bulk insert from a table**
- **Proper error handling**

## License
This project is licensed under the MIT License.

**Thanks!** 🚀

