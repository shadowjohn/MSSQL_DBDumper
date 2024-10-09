using App;
using GFLib.Database;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using utility;
namespace DBDumper
{
    internal class Program
    {
        static string VERSION = "0.02";
        public static myinclude my = new myinclude();
        static myApp app = new myApp();
        static bool showHelp = false;
        static string host = "";
        static string port = "1433";
        static string user = "";
        static string password = "";
        static string database = "";
        public static string filter_SQL = ""; //可以下額外的 SQL 條件
        public static long run_rows = 100000;
        public static string table = "";
        static bool is_append = true;
        public static List<string> mssql_table_columns = new List<string>(); //取得的資料庫欄位名
        static string output = "output.sqlite"; // 默認輸出文件名

        public static string outputFormat = "sqlite"; // 默認輸出格式 也可以是 mssql


        public static string SQLitePath = output; // 最後SQLite 位置
        static void Main(string[] args)
        {
            long st = Convert.ToInt64(Program.my.time());
            // 解析命令行參數
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "-h":
                    case "-help":
                        showHelp = true;
                        break;
                    case "-host":
                        host = args[++i];
                        break;
                    case "-port":
                        port = args[++i];
                        break;
                    case "-user":
                    case "-u":
                        user = args[++i];
                        break;
                    case "-rows":
                        run_rows = Convert.ToInt64(args[++i]);
                        break;
                    case "-password":
                    case "-p":
                        password = args[++i];
                        break;
                    case "-database":
                        database = args[++i];
                        database = database.Replace("[", "").Replace("]", "").Replace("`", "").Replace("'", "").Replace("\"", "");
                        break;
                    case "-table":
                        table = args[++i];
                        table = table.Replace("[", "").Replace("]", "").Replace("`", "").Replace("'", "").Replace("\"", "");
                        break;
                    case "-SQL":
                        filter_SQL = args[++i];
                        break;
                    case "-overwrite":
                        is_append = false;
                        break;
                    case "-output":
                    case "-o":
                        output = args[++i];
                        break;
                    case "-format":
                        outputFormat = args[++i];
                        break;
                }
            }

            // 提示用戶輸入參數
            if (string.IsNullOrEmpty(host))
            {
                //Console.WriteLine("Enter host: ");
                //host = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(port))
            {
                //Console.WriteLine("Enter port: ");
                //port = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(user))
            {
                //Console.WriteLine("Enter username: ");
                //user = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(password))
            {
                //Console.WriteLine("Enter password: ");
                //password = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(database))
            {
                //Console.WriteLine("Enter database name: ");
                //database = Console.ReadLine();
            }

            if (string.IsNullOrEmpty(table))
            {
                //Console.WriteLine("Enter table name: ");
                //table = Console.ReadLine();
            }

            if (showHelp || string.IsNullOrEmpty(host) || string.IsNullOrEmpty(port) || string.IsNullOrEmpty(user) ||
                string.IsNullOrEmpty(password) || string.IsNullOrEmpty(database) || string.IsNullOrEmpty(table))
            {
                Console.WriteLine("Mssql to SQLite dumper");
                Console.WriteLine("Author: FeatherMountain (https://3wa.tw)");
                Console.WriteLine("Version: " + VERSION);
                Console.WriteLine("  Usage:");
                Console.WriteLine("   -host       The host name.");
                Console.WriteLine("   -port       The port number.");
                Console.WriteLine("   -user, -u   The user name.");
                Console.WriteLine("   -password, -p   The password.");
                Console.WriteLine("   -database   The database name.");
                Console.WriteLine("   -table      The table name.");
                Console.WriteLine("   -SQL        Additional SQL conditions. Example -SQL \" [computers_id] = '5566' \" ");
                Console.WriteLine("   -overwrite  Overwrite to sqlite. Default append mode.");
                Console.WriteLine("   -rows       Eveytime dump rows. Default " + run_rows.ToString());
                //2024-10-08 加入可指定輸出 MSSQL 語法 only
                Console.WriteLine("   -format     [Default: sqlite] Output format, could be mssql_sql .");


                Console.WriteLine("   -output, -o [Default: output.sqlite] The output SQLite file name. Or output SQL txt file. ");
                Console.WriteLine("");
                Console.WriteLine(" Example:");
                Console.WriteLine("   DBDumper.exe -host 3wa.tw -port 1433 -u ooxx -p ooxx -database test -table testtable -o output.sqlite");
                Console.WriteLine("   DBDumper.exe -host 3wa.tw -port 1433 -u ooxx -p ooxx -database test -table testtable -format mssql_sql -o output.sql");
                return;
            }
            SQLitePath = output;
            if (my.is_string_like(output, "\\") || my.is_string_like(output, "/"))
            {
            }
            else
            {
                SQLitePath = my.pwd() + "\\" + my.basename(output);
            }
            string sn = my.subname(SQLitePath);
            if (!my.in_array(sn.ToLower(), new List<string>() { "db", "sqlite", "data", "sql", "txt" }))
            {
                Console.WriteLine("Output file should be: db, sqlite, data, sql, txt ...");
                return;
            }

            if (is_append == false)
            {
                if (my.is_file(SQLitePath))
                {
                    my.unlink(SQLitePath);
                }
            }

            //try link db
            my.linkToDB(host, port, database, user, password);

            //Try
            try
            {
                my.selectSQL_SAFE("SELECT 1");
            }
            catch
            {
                Console.WriteLine("Connection issue...");
                return;
            }
            //讀出 mssql 結構
            string SQL = @"
                SELECT t.name AS table_name, c.name AS column_name, 
                (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name ) AS [DATA_TYPE],
                (SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name) AS [IS_NULLABLE], 
                (SELECT (COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity')) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TABLE_NAME AND COLUMN_NAME = c.name) AS [IsIdentity],
                isnull(dc.definition,'') AS [default_value],
                ep.value AS [comments]                
                FROM sys.tables t
                INNER JOIN sys.columns c ON t.object_id = c.object_id
                LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.class = 1 AND ep.name = 'MS_Description'
                LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                where t.name = @TABLE_NAME
            ";
            var pa = new Dictionary<string, string>();
            pa["TABLE_NAME"] = table;
            var jd = my.selectSQL_SAFE(SQL, pa);
            if (jd.Rows.Count == 0)
            {
                Console.WriteLine(" No such table..." + table);
                return;
            }
            for (int i = 0, max_i = jd.Rows.Count; i < max_i; i++)
            {
                mssql_table_columns.Add(jd.Rows[i]["column_name"].ToString());
            }
            //Console.WriteLine(my.json_encode(jd));

            outputFormat = outputFormat.ToLower();
            //如果 outputFormat 是 mssql_sql，就輸出 匯出 mssql create table、insert 語法
            if (outputFormat == "mssql_sql")
            {
                Console.WriteLine("Exporting to MSSQL SQL...");
                //讀出 mssql 結構

                string tableSchema = my.dumpMSSQL_Schema(database, table);

                // 看有沒有 pk 欄位
                string pk = my.getPK(database, table);

                // 輸出檔案
                my.file_put_contents(SQLitePath, tableSchema);

                my.file_put_contents(SQLitePath, "SET IDENTITY_INSERT [" + table + "] ON;\n", true);
                // 讀出 mssql 資料
                // 依每次 rows 數量，分批次讀取,產生 INSERT 語法,並append 到檔案
                // 取得總筆數
                long total_rows = my.countMSSQL_SQL_Data(database, table, filter_SQL);
                long offset = 0;
                long limit = run_rows;
                while (true)
                {
                    string insertSQL = my.dumpMSSQL_SQL_Data(database, table, mssql_table_columns, pk, offset, limit, filter_SQL);
                    if (insertSQL == null || insertSQL == "")
                    {
                        //Console.WriteLine("Exporting to MSSQL SQL... " + (insertSQL == null));
                        break;
                    }
                    my.file_put_contents(SQLitePath, insertSQL + "\n", true);
                    offset += limit;
                    // echo
                    Console.WriteLine("Exporting to MSSQL SQL... " + offset.ToString() + " / " + total_rows.ToString());
                }
                my.file_put_contents(SQLitePath, "SET IDENTITY_INSERT [" + table + "] OFF;\n", true);
                long _ts = Convert.ToInt64(Program.my.time());
                Console.WriteLine("開始時間: " + my.date("Y-m-d H:i:s", st.ToString()));
                Console.WriteLine("結束時間: " + my.date("Y-m-d H:i:s", _ts.ToString()));
                Console.WriteLine("花費時間: " + my.secondtodhis((_ts - st)));
                Console.WriteLine("匯出完成... " + SQLitePath);
                return;

            }




            using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + SQLitePath + ";Version=3;"))
            {
                connection.Open();
                SQL = @"
                    SELECT `name` FROM `sqlite_master` WHERE type='table' AND `name` = @TABLE_NAME;
                ";
                pa.Clear();
                pa["TABLE_NAME"] = table;
                var sra = my.SQLite_selectSQL_SAFE(connection, SQL, pa);
                if (sra.Rows.Count == 0)
                {
                    //sqlite 沒有這筆，要建立
                    //""id"" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
                    //""name"" TEXT
                    /*
                        CREATE TABLE "main"."Untitled" (
                          "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
                          "name" TEXT NOT NULL DEFAULT '5',
                          "xxx" text
                        );
                    */
                    SQL = @"
                        CREATE TABLE `" + table + @"` (
                            -- `__BY_3WA_IS_OK__` INTEGER DEFAULT '0',
                            ";
                    for (int i = 0, max_i = jd.Rows.Count; i < max_i; i++)
                    {
                        string DATA_TYPE = my.MapDataType(jd.Rows[i]["DATA_TYPE"].ToString());
                        string null_string = "NOT NULL";
                        if (jd.Rows[i]["IS_NULLABLE"].ToString() == "YES")
                        {
                            null_string = "";
                        }
                        //default value
                        jd.Rows[i]["default_value"] = jd.Rows[i]["default_value"].ToString().Replace("(", "").Replace(")", "");
                        jd.Rows[i]["default_value"] = (jd.Rows[i]["default_value"].ToString() == "") ? "" : "'" + jd.Rows[i]["default_value"].ToString() + "'";
                        jd.Rows[i]["default_value"] = jd.Rows[i]["default_value"].ToString().Replace("''", "'");
                        jd.Rows[i]["default_value"] = (jd.Rows[i]["default_value"].ToString() == "") ? "" : "DEFAULT " + jd.Rows[i]["default_value"].ToString();
                        string PK = "";
                        if (jd.Rows[i]["IsIdentity"].ToString() == "1")
                        {
                            PK = "PRIMARY KEY AUTOINCREMENT";
                        }
                        SQL += @"`" + jd.Rows[i]["column_name"].ToString() + "` " + DATA_TYPE + " " + null_string + " " + jd.Rows[i]["default_value"].ToString() + " " + PK;
                        if (i != max_i - 1)
                        {
                            SQL += ",";
                        }
                    }
                    SQL += @"
                        );
                    ";
                    //Console.WriteLine(SQL);
                    //建立
                    Console.WriteLine("1. 建立資料表... " + table);
                    my.SQLite_execSQL_SAFE(connection, SQL, new Dictionary<string, string>());

                    Console.WriteLine("2. SQLite 建立 INDEX... " + table);
                    //參考原MSSQL INDEX 資料，建立 SQLite INDEX
                    /*
                     CREATE INDEX "main"."INDEX,computers_id"
                        ON "hdd_log_copy1" (
                          "INDEX",
                          "computers_id"
                        );
                    */
                    // 檢查有沒有 index 
                    Dictionary<string, List<string>> INDEX_fields = Program.my.mssql_get_table_indexs(Program.table);
                    if (INDEX_fields != null)
                    {
                        //有 INDEX
                        foreach (string k in INDEX_fields.Keys)
                        {
                            SQL = @"
                                CREATE INDEX ""main"".""" + k + @"""
                                ON """ + table + @""" (
                                  """ + my.implode("\",\"", INDEX_fields[k]) + @"""
                                );
                            ";
                            Console.WriteLine("SQLite 建立 INDEX: " + k);
                            my.SQLite_execSQL_SAFE(connection, SQL, new Dictionary<string, string>());
                        }
                    }
                    //my.SQLite_execSQL_SAFE(connection, SQL, new Dictionary<string, string>());
                } // if 沒表時
            }; //using 
            Console.WriteLine("3. 檢查 MSSQL " + table + " 與 SQLite 筆數是否相同...");
            Console.WriteLine("若筆數多，需要稍長的時間...");
            if (!app.run_step1_check_mssql_sqlite_rows())
            {
                // 不同時，要先把 index 建好
                Console.WriteLine("4. SQLite 筆數與 MSSQL 不同，需要比對處理");
                app.run_step2_sqlite_set_ids();
            }
            //Console.WriteLine(my.json_encode(sra));
            long ts = Convert.ToInt64(Program.my.time());
            Console.WriteLine("開始時間: " + my.date("Y-m-d H:i:s", st.ToString()));
            Console.WriteLine("結束時間: " + my.date("Y-m-d H:i:s", ts.ToString()));
            Console.WriteLine("花費時間: " + my.secondtodhis((ts - st)));
            Console.WriteLine("匯出完成... " + SQLitePath);
        }
    }
}
