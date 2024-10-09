using GFLib.Database;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
namespace utility
{
    public class myinclude
    {
        private string connString = "";
        private Random rnd = new Random(DateTime.Now.Millisecond);
        public myinclude()
        {
            ServicePointManager.ServerCertificateValidationCallback = delegate { return true; };
            ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
        }
        public void linkToDB(string host, string port, string database, string user, string password)
        {
            connString = "Data Source=" + host + "," + port + "; Initial Catalog=" + database + ";Persist Security Info=True;User ID=" + user + ";Password=" + password;
            //Console.WriteLine(connString);
            //return new GFLib.Database.MsSql(connString, -1);
        }
        private MsSql linkToDB()
        {
            return new GFLib.Database.MsSql(connString, -1);
        }
        public DataTable selectSQL_SAFE(string SQL)
        {
            return selectSQL_SAFE(SQL, new Dictionary<string, string>());
        }
        public string mssql_get_table_pk(string table)
        {
            string SQL = @"
                SELECT [COLUMN_NAME]
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @tableName AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1;
            ";
            var pa = new Dictionary<string, string>();
            pa["tableName"] = table;
            var ra = selectSQL_SAFE(SQL, pa);
            if (ra.Rows.Count != 0)
            {
                return ra.Rows[0]["COLUMN_NAME"].ToString();
            }
            return null;
        }
        public Dictionary<string, List<string>> mssql_get_table_indexs(string table)
        {
            string SQL = @"
                SELECT 
                    i.name AS IndexName,
                    OBJECT_NAME(i.object_id) AS TableName,
                    c.name AS ColumnName
                FROM 
                    sys.indexes i
                INNER JOIN 
                    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN 
                    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN 
                    sys.tables t ON i.object_id = t.object_id
                WHERE 
                    t.name = @tableName
                ORDER BY i.name ASC
            ";
            Dictionary<string, List<string>> output = new Dictionary<string, List<string>>();
            var pa = new Dictionary<string, string>();
            pa["tableName"] = table;
            var ra = selectSQL_SAFE(SQL, pa);
            if (ra.Rows.Count != 0)
            {
                for (int i = 0, max_i = ra.Rows.Count; i < max_i; i++)
                {
                    if (output.ContainsKey(ra.Rows[i]["IndexName"].ToString()) == false)
                    {
                        output[ra.Rows[i]["IndexName"].ToString()] = new List<string>();
                    }
                    if (!in_array(ra.Rows[i]["ColumnName"].ToString(), output[ra.Rows[i]["IndexName"].ToString()]))
                    {
                        output[ra.Rows[i]["IndexName"].ToString()].Add(ra.Rows[i]["ColumnName"].ToString());
                    }
                }
                return output;
            }
            else
            {
                return null;
            }
        }
        public DataTable selectSQL_SAFE(string SQL, Dictionary<string, string> m)
        {

            var PDO = linkToDB();
            var pa = new ArrayList();
            List<string> fields = new List<string>();
            List<string> Q_fields = new List<string>();
            foreach (string n in m.Keys)
            {
                fields.Add(n);
                Q_fields.Add("@" + n);
                pa.Add(new SqlParameter { ParameterName = "@" + n, SqlDbType = SqlDbType.NVarChar, Value = m[n] });
            }
            var ra = PDO.Select(SQL, pa);
            PDO.Dispose();
            PDO = null;
            return ra;
        }
        public DataTable SQLite_selectSQL_SAFE(string sqliteFile, string SQL, Dictionary<string, string> m)
        {
            using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + sqliteFile + ";Version=3;"))
            {
                connection.Open();
                return SQLite_selectSQL_SAFE(connection, SQL, m);
            }
        }
        public string date(string format)
        {
            return date(format, strtotime(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff")));
        }
        public string secondtodhis(long time)
        {
            //秒數轉成　天時分秒
            //Create by 羽山 
            // 2010-02-07
            string days = string.Format("{0}", time / (24 * 60 * 60));
            days = (Convert.ToInt32(days) >= 1) ? days + "天" : "";
            string hours = string.Format("{0}", (time % (60 * 60 * 24)) / (60 * 60));
            hours = (days == "" && hours == "0") ? "" : hours + "時";
            string mins = string.Format("{0}", (time % (60 * 60)) / (60));
            mins = (days == "" && hours == "" && mins == "0") ? "" : mins + "分";
            string seconds = string.Format("{0}", (time % 60)) + "秒";
            string output = string.Format("{0}{1}{2}{3}", days, hours, mins, seconds);
            return output;
        }
        public string date(string format, string unixtimestamp)
        {
            DateTime tmp = UnixTimeToDateTime(unixtimestamp);
            tmp = tmp.AddHours(+8);
            switch (format)
            {
                case "Y-m-d H:i:s":
                    return tmp.ToString("yyyy-MM-dd HH:mm:ss");
                case "Y/m/d":
                    return tmp.ToString("yyyy/MM/dd");
                case "Y/m/d H:i:s":
                    return tmp.ToString("yyyy/MM/dd HH:mm:ss");
                case "Y/m/d H:i:s.fff":
                    return tmp.ToString("yyyy/MM/dd HH:mm:ss.fff");
                case "Y-m-d_H_i_s":
                    return tmp.ToString("yyyy-MM-dd_HH_mm_ss");
                case "Y-m-d":
                    return tmp.ToString("yyyy-MM-dd");
                case "H:i:s":
                    return tmp.ToString("HH:mm:ss");
                case "Y-m-d H:i":
                    return tmp.ToString("yyyy-MM-dd HH:mm");
                case "Y_m_d_H_i_s":
                    return tmp.ToString("yyyy_MM_dd_HH_mm_ss");
                case "Y_m_d_H_i_s_fff":
                    return tmp.ToString("yyyy_MM_dd_HH_mm_ss_fff");
                case "w":
                    //回傳week, sun =0 , sat = 6, mon=1.....
                    return Convert.ToInt16(tmp.DayOfWeek).ToString();
                case "Y":
                    return tmp.ToString("yyyy");
                case "m":
                    return tmp.ToString("MM");
                case "d":
                    return tmp.ToString("dd");
                case "H":
                    return tmp.ToString("HH");
                case "i":
                    return tmp.ToString("mm");
                case "s":
                    return tmp.ToString("ss");
                case "Y-m-d H:i:s.fff":
                    return tmp.ToString("yyyy-MM-dd HH:mm:ss.fff");
                case "Y-m-d H:i:s.ffffff":
                    return tmp.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                case "H:i:s.fff":
                    return tmp.ToString("HH:mm:ss.fff");
                case "H:i:s.ffffff":
                    return tmp.ToString("HH:mm:ss.ffffff");
            }
            return "";
        }
        public DataTable SQLite_selectSQL_SAFE(SQLiteConnection PDO, string SQL, Dictionary<string, string> m)
        {

            DataTable ra = new DataTable();
            using (SQLiteCommand command = new SQLiteCommand(SQL, PDO))
            {
                // 添加参数
                foreach (string n in m.Keys)
                {
                    command.Parameters.AddWithValue("@" + n, m[n]);
                }
                using (SQLiteDataAdapter adapter = new SQLiteDataAdapter(command))
                {
                    adapter.Fill(ra);
                }
            }
            return ra;
        }
        public string strtotime(string value)
        {
            //create Timespan by subtracting the value provided from
            //the Unix Epoch
            TimeSpan span = (Convert.ToDateTime(value) - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());

            //return the total seconds (which is a UNIX timestamp)
            if (is_string_like(value, "."))
            {
                //有小數點               
                double sec = span.Ticks / (TimeSpan.TicksPerMillisecond / 1000.0) / 1000000.0;
                return sec.ToString();
            }
            else
            {
                return span.TotalSeconds.ToString();
            }
        }
        public string strtotime(DateTime value)
        {
            //create Timespan by subtracting the value provided from
            //the Unix Epoch
            TimeSpan span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());

            //return the total seconds (which is a UNIX timestamp)
            return span.TotalSeconds.ToString();
        }
        public string my_money_format(double data, int n)
        {
            string dataStr = data.ToString("#,##0");

            string[] parts = data.ToString().Split('.');
            string dataDecimalPart = parts.Length > 1 ? parts[1] : "";

            if (dataDecimalPart == "0")
            {
                return dataStr;
            }
            else
            {
                string formattedDecimalPart = dataDecimalPart.Length > n ? dataDecimalPart.Substring(0, n) : dataDecimalPart;
                if (formattedDecimalPart == "")
                {
                    return $"{dataStr}";
                }
                else
                {
                    return $"{dataStr}.{formattedDecimalPart}";
                }
            }
        }
        public DateTime UnixTimeToDateTime(string text)
        {
            System.DateTime dateTime = new System.DateTime(1970, 1, 1, 0, 0, 0, 0);
            // Add the number of seconds in UNIX timestamp to be converted.            
            dateTime = dateTime.AddSeconds(Convert.ToDouble(text));
            return dateTime;
        }
        public string time()
        {
            return strtotime(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
        }
        public int SQLite_execSQL_SAFE(SQLiteConnection PDO, string SQL, Dictionary<string, string> m)
        {
            using (SQLiteCommand command = new SQLiteCommand(SQL, PDO))
            {
                // 添加参数
                foreach (string n in m.Keys)
                {
                    command.Parameters.AddWithValue("@" + n, m[n]);
                }
                return command.ExecuteNonQuery();
            }
        }
        public string implode(string keyword, string[] arrays)
        {
            return string.Join(keyword, arrays);
        }
        public string implode(string keyword, List<string> arrays)
        {
            return string.Join<string>(keyword, arrays);
        }
        public string implode(string keyword, Dictionary<int, string> arrays)
        {
            string[] tmp = new String[arrays.Keys.Count];
            int i = 0;
            foreach (int k in arrays.Keys)
            {
                tmp[i++] = arrays[k];
            }
            return string.Join(keyword, tmp);
        }
        public string implode(string keyword, Dictionary<string, string> arrays)
        {
            string[] tmp = new String[arrays.Keys.Count];
            int i = 0;
            foreach (string k in arrays.Keys)
            {
                tmp[i++] = arrays[k];
            }
            return string.Join(keyword, tmp);
        }
        public string implode(string keyword, ArrayList arrays)
        {
            string[] tmp = new String[arrays.Count];
            for (int i = 0; i < arrays.Count; i++)
            {
                tmp[i] = arrays[i].ToString();
            }
            return string.Join(keyword, tmp);
        }
        public Int64 SQLite_insertSQL(SQLiteConnection PDO, string table, Dictionary<string, string> m)
        {
            var pa = new ArrayList();
            List<string> fields = new List<string>();
            List<string> Q_fields = new List<string>();
            foreach (string n in m.Keys)
            {
                fields.Add(n);
                Q_fields.Add("@" + n);
            }
            string SQL = @"
                INSERT INTO `" + table + @"`(`" + implode("`,`", fields) + "`)VALUES(" + implode(",", Q_fields) + @")
            ";
            using (SQLiteCommand command = new SQLiteCommand(SQL, PDO))
            {
                foreach (string n in m.Keys)
                {
                    command.Parameters.AddWithValue(n, m[n]);
                }
                command.ExecuteNonQuery();
                command.CommandText = "SELECT last_insert_rowid()";
                return Convert.ToInt64(command.ExecuteScalar());
            }
        }
        public string EscapeUnicode(string input)
        {
            StringBuilder sb = new StringBuilder(input.Length);
            foreach (char ch in input)
            {
                if (ch <= 0x7f)
                    sb.Append(ch);
                else
                    sb.AppendFormat(CultureInfo.InvariantCulture, "\\u{0:x4}", (int)ch);
            }
            return sb.ToString();
        }
        public string unEscapeUnicode(string input)
        {
            return Regex.Unescape(input);
        }
        public string json_encode(object input)
        {
            return EscapeUnicode(JsonConvert.SerializeObject(input, Formatting.None));
            //return JsonConvert.SerializeObject(input, Formatting.None);
        }
        // 将 MSSQL 数据类型映射到 SQLite 数据类型
        public string MapDataType(string dataType)
        {
            switch (dataType.ToLower())
            {
                case "int":
                case "bigint":
                case "smallint":
                case "tinyint":
                    return "INTEGER";
                case "nvarchar":
                case "varchar":
                case "text":
                    return "TEXT";
                case "datetime2":
                case "datetime":
                case "date":
                case "time":
                    return "DATETIME";
                case "decimal":
                case "numeric":
                case "float":
                    return "REAL";
                default:
                    return "BLOB";
            }
        }
        public string pwd()
        {
            //return dirname(System.Web.HttpContext.Current.Request.PhysicalPath);
            return System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetEntryAssembly().Location);
        }
        public bool in_array(string find_key, List<string> arr)
        {
            return arr.Contains(find_key);
        }
        public bool in_array(string find_key, string[] arr)
        {
            return arr.Contains(find_key);
        }
        public bool in_array(string find_key, char[] arr)
        {
            string[] o = new string[arr.Count()];
            for (int i = 0; i < arr.Count(); i++)
            {
                o[i] = arr[i].ToString();
            }
            return in_array(find_key, o);
        }
        public bool in_array(string find_key, ArrayList arr)
        {
            return arr.Contains(find_key);
        }
        public bool is_string_like(string data, string find_string)
        {
            return (data.IndexOf(find_string) == -1) ? false : true;
        }
        public string basename(string path)
        {
            return Path.GetFileName(path);
        }
        public string mainname(string path)
        {
            return Path.GetFileNameWithoutExtension(path);
        }
        public string subname(string path)
        {
            return Path.GetExtension(path).TrimStart('.');
        }
        public bool is_dir(string path)
        {
            return Directory.Exists(path);
        }
        public bool is_file(string filepath)
        {
            return File.Exists(filepath);
        }
        public void unlink(string filepath)
        {
            if (is_file(filepath))
            {
                File.Delete(filepath);
            }
        }
        public Dictionary<string, List<string>> mssql_get_table_indexs(string databaseName, string table)
        {
            string SQL = @"
                USE [" + databaseName + @"];
                SELECT 
                    i.name AS IndexName,
                    OBJECT_NAME(i.object_id) AS TableName,
                    c.name AS ColumnName
                FROM 
                    sys.indexes i
                INNER JOIN 
                    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN 
                    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN 
                    sys.tables t ON i.object_id = t.object_id
                WHERE 
                    t.name = @tableName
                ORDER BY i.name ASC
            ";
            Dictionary<string, List<string>> output = new Dictionary<string, List<string>>();
            var pa = new Dictionary<string, string>();
            pa["tableName"] = table;
            var ra = selectSQL_SAFE(SQL, pa);
            if (ra.Rows.Count != 0)
            {
                for (int i = 0, max_i = ra.Rows.Count; i < max_i; i++)
                {
                    if (output.ContainsKey(ra.Rows[i]["IndexName"].ToString()) == false)
                    {
                        output[ra.Rows[i]["IndexName"].ToString()] = new List<string>();
                    }
                    if (!in_array(ra.Rows[i]["ColumnName"].ToString(), output[ra.Rows[i]["IndexName"].ToString()]))
                    {
                        output[ra.Rows[i]["IndexName"].ToString()].Add(ra.Rows[i]["ColumnName"].ToString());
                    }
                }
                return output;
            }
            else
            {
                return null;
            }
        }
        public byte[] s2b(string input)
        {
            return System.Text.Encoding.UTF8.GetBytes(input);
        }
        public void file_put_contents(string filepath, string input)
        {
            file_put_contents(filepath, s2b(input), false);
        }
        public void file_put_contents(string filepath, byte[] input)
        {
            file_put_contents(filepath, input, false);
        }
        public void file_put_contents(string filepath, string input, bool isFileAppend)
        {
            file_put_contents(filepath, s2b(input), isFileAppend);
        }
        public void file_put_contents(string filepath, byte[] input, bool isFileAppend)
        {
            FileMode mode = isFileAppend ? (File.Exists(filepath) ? FileMode.Append : FileMode.Create) : FileMode.Create;

            try
            {
                using (FileStream myFile = new FileStream(filepath, mode, FileAccess.Write, FileShare.None))
                {
                    myFile.Write(input, 0, input.Length);
                }
            }
            catch (Exception ex)
            {
                // 處理例外情況，例如記錄錯誤信息
                Console.WriteLine($"An error occurred: {ex.Message}");
            }
        }
        public long countMSSQL_SQL_Data(string database, string table, string filter_SQL)
        {
            // 計算 MSSQL 資料筆數
            string SQL = @"
                USE [" + database + @"];
                SELECT COUNT(1) AS [COUNTER]
                FROM [" + table + @"]
            ";
            if (filter_SQL != "")
            {
                SQL += " WHERE " + filter_SQL;
            }

            var ra = selectSQL_SAFE(SQL);
            if (ra.Rows.Count != 0)
            {
                return Convert.ToInt64(ra.Rows[0]["COUNTER"]);
            }
            return 0;
        }
        public string getPK(string database, string table)
        {
            string PK = null;
            string SQL = @"
                USE [" + database + @"];
                SELECT [COLUMN_NAME]
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @tableName AND COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1;
            ";
            var pa = new Dictionary<string, string>();
            pa["tableName"] = table;
            var ra = selectSQL_SAFE(SQL, pa);
            if (ra.Rows.Count != 0)
            {
                PK = ra.Rows[0]["COLUMN_NAME"].ToString();
            }
            return PK;
        }
        public string dumpMSSQL_SQL_Data(string database, string table, List<string> mssql_table_columns, string pk, long offset, long limit, string filter_SQL)
        {
            // 產出 MSSQL 資料的 INSERT SQL 語法
            // 如果該欄位是 datetime 或 datetime2 匯出時,要 Convert 成 120 格式
            // 例如: CONVERT(varchar(24), [欄位名稱], 120) AS [欄位名稱]
            // 取得資料表的型態

            var field_dic = dumpMSSQL_Schema_dic(database, table);
            if (field_dic == null)
            {
                Console.WriteLine("Table not found");
                return null;
            }
            List<string> do_mssql_table_columns = new List<string>();
            for (int i = 0, max_i = mssql_table_columns.Count(); i < max_i; i++)
            {
                do_mssql_table_columns.Add(mssql_table_columns[i]);
            }
            for (int i = 0, max_i = do_mssql_table_columns.Count(); i < max_i; i++)
            {
                if (field_dic.ContainsKey(do_mssql_table_columns[i]) == false)
                {
                    return null;
                }
                string dataType = field_dic[do_mssql_table_columns[i]]["DATA_TYPE"];
                if (in_array(dataType, new List<string> { "datetime", "datetime2" }))
                {
                    do_mssql_table_columns[i] = "CONVERT(varchar(30), [" + do_mssql_table_columns[i] + "], 120) AS [" + do_mssql_table_columns[i] + "]";
                }
                else
                {
                    do_mssql_table_columns[i] = "[" + do_mssql_table_columns[i] + "]";
                }

            }

            string SQL = @"
                USE [" + database + @"];
                SELECT " + implode(",\n", do_mssql_table_columns) + @"
                FROM [" + table + @"]
            ";
            if (filter_SQL != "")
            {
                SQL += " WHERE " + filter_SQL;
            }
            string _pk = pk;
            if (_pk == null)
            {
                //改用 index 嗎...
                _pk = do_mssql_table_columns[0];
            }

            SQL += @"
                ORDER BY " + _pk + @" ASC
                OFFSET " + offset + @" ROWS
                FETCH NEXT " + limit + @" ROWS ONLY;
            ";
            //Console.WriteLine(SQL);
            var ra = selectSQL_SAFE(SQL);
            if (ra.Rows.Count == 0)
            {
                Console.WriteLine("No Data");
                return null;
            }
            List<string> SQLs = new List<string>();
            for (int i = 0, max_i = ra.Rows.Count; i < max_i; i++)
            {
                List<string> values = new List<string>();
                List<string> ColumnNames = new List<string>();
                for (int j = 0, max_j = ra.Columns.Count; j < max_j; j++)
                {
                    string column = ra.Columns[j].ColumnName;
                    //Console.WriteLine(column);
                    var value = ra.Rows[i][column];
                    if (value == null)
                    {
                        values.Add("NULL");
                    }
                    else
                    {
                        string v = value.ToString();
                        values.Add("'" + v.Replace("'", "''") + "'");
                    }
                    ColumnNames.Add(column);
                }
                SQLs.Add("INSERT INTO [" + table + "] ([" + implode("],[", ColumnNames) + "]) VALUES (" + implode(",", values) + ");");
            }
            return implode("\n", SQLs);
        }
        public Dictionary<string, Dictionary<string, string>> dumpMSSQL_Schema_dic(string databaseName, string tableName)
        {
            string SQL = @"
                    USE [" + databaseName + @"];
                    SELECT t.name AS table_name, c.name AS column_name, 
                    (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name ) AS [DATA_TYPE],
                    (SELECT CHARACTER_MAXIMUM_LENGTH  FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name ) AS [CHARACTER_MAXIMUM_LENGTH],
                    (SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name) AS [IS_NULLABLE], 
                    (SELECT (COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity')) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TABLE_NAME AND COLUMN_NAME = c.name) AS [IsIdentity],
                    isnull(dc.definition,'') AS [default_value],
                    isnull(ep.value,'') AS [comments],
                    CASE 
                        WHEN pk.column_id IS NOT NULL THEN 'YES' 
                        ELSE 'NO' 
                    END AS IsPrimaryKey                            
                    FROM sys.tables t
                    INNER JOIN sys.columns c ON t.object_id = c.object_id
                    LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.class = 1 AND ep.name = 'MS_Description'
                    LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                    LEFT JOIN 
                    ( -- 查找主键信息
                        SELECT 
                        kc.parent_object_id, 
                        ic.column_id 
                        FROM 
                        sys.key_constraints kc
                        INNER JOIN 
                        sys.index_columns ic 
                        ON kc.unique_index_id = ic.index_id 
                            AND kc.parent_object_id = ic.object_id
                        WHERE 
                        kc.type = 'PK' -- 主键约束类型
                    ) pk 
                    ON c.object_id = pk.parent_object_id 
                        AND c.column_id = pk.column_id
                    where t.name = @TABLE_NAME
                ";
            var pa = new Dictionary<string, string>();
            pa["TABLE_NAME"] = tableName;
            var jd = selectSQL_SAFE(SQL, pa);
            if (jd.Rows.Count == 0)
            {
                return null;
            }
            Dictionary<string, Dictionary<string, string>> output = new Dictionary<string, Dictionary<string, string>>();
            for (int j = 0, max_j = jd.Rows.Count; j < max_j; j++)
            {
                var column = jd.Rows[j];
                Dictionary<string, string> columnDefinition = new Dictionary<string, string>();
                columnDefinition["DATA_TYPE"] = column["DATA_TYPE"].ToString();
                columnDefinition["CHARACTER_MAXIMUM_LENGTH"] = column["CHARACTER_MAXIMUM_LENGTH"].ToString();
                columnDefinition["IS_NULLABLE"] = column["IS_NULLABLE"].ToString();
                columnDefinition["IsIdentity"] = column["IsIdentity"].ToString();
                columnDefinition["default_value"] = column["default_value"].ToString();
                columnDefinition["comments"] = column["comments"].ToString();
                columnDefinition["IsPrimaryKey"] = column["IsPrimaryKey"].ToString();
                output[column["column_name"].ToString()] = columnDefinition;
            }
            return output;
        }
        public string dumpMSSQL_Schema(string databaseName, string tableName)
        {
            string SQL = @"
                    USE [" + databaseName + @"];
                    SELECT t.name AS table_name, c.name AS column_name, 
                    (SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name ) AS [DATA_TYPE],
                    (SELECT CHARACTER_MAXIMUM_LENGTH  FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name ) AS [CHARACTER_MAXIMUM_LENGTH],
                    (SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = t.name and COLUMN_NAME = c.name) AS [IS_NULLABLE], 
                    (SELECT (COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity')) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TABLE_NAME AND COLUMN_NAME = c.name) AS [IsIdentity],
                    isnull(dc.definition,'') AS [default_value],
                    isnull(ep.value,'') AS [comments],
                    CASE 
                        WHEN pk.column_id IS NOT NULL THEN 'YES' 
                        ELSE 'NO' 
                    END AS IsPrimaryKey                            
                    FROM sys.tables t
                    INNER JOIN sys.columns c ON t.object_id = c.object_id
                    LEFT JOIN sys.extended_properties ep ON ep.major_id = c.object_id AND ep.minor_id = c.column_id AND ep.class = 1 AND ep.name = 'MS_Description'
                    LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
                    LEFT JOIN 
                    ( -- 查找主键信息
                        SELECT 
                        kc.parent_object_id, 
                        ic.column_id 
                        FROM 
                        sys.key_constraints kc
                        INNER JOIN 
                        sys.index_columns ic 
                        ON kc.unique_index_id = ic.index_id 
                            AND kc.parent_object_id = ic.object_id
                        WHERE 
                        kc.type = 'PK' -- 主键约束类型
                    ) pk 
                    ON c.object_id = pk.parent_object_id 
                        AND c.column_id = pk.column_id
                    where t.name = @TABLE_NAME
                ";
            var pa = new Dictionary<string, string>();
            pa["TABLE_NAME"] = tableName;
            var jd = selectSQL_SAFE(SQL, pa);
            if (jd.Rows.Count == 0)
            {
                return null;
            }
            StringBuilder createTableScript = new StringBuilder();
            createTableScript.AppendLine("USE [" + databaseName + "];");
            createTableScript.AppendLine("IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '" + tableName + "') ");
            createTableScript.AppendLine("BEGIN");
            createTableScript.AppendLine("  CREATE TABLE dbo.[" + tableName + "] (");

            bool hasPK = false;
            string PK_field = "";
            for (int j = 0, max_j = jd.Rows.Count; j < max_j; j++)
            {
                var column = jd.Rows[j];
                string columnDefinition = "[" + column["column_name"].ToString() + "] " + column["DATA_TYPE"].ToString();


                if (in_array(column["DATA_TYPE"].ToString(), new List<string> { "char", "nchar", "varchar", "nvarchar" }))
                {
                    switch (column["CHARACTER_MAXIMUM_LENGTH"].ToString())
                    {
                        case "-1":
                            columnDefinition += "(max)";
                            break;
                        default:
                            columnDefinition += "(" + column["CHARACTER_MAXIMUM_LENGTH"].ToString() + ")";
                            break;
                    }
                }

                if (column["IsIdentity"].ToString() == "1")
                {
                    columnDefinition += " IDENTITY(1,1)";
                }

                if (column["IS_NULLABLE"].ToString() == "NO")
                {
                    columnDefinition += " NOT NULL";
                }

                if (column["default_value"].ToString() != "")
                {
                    columnDefinition += " DEFAULT " + column["default_value"].ToString();
                }

                createTableScript.AppendLine("    " + columnDefinition + ",");

                if (column["IsPrimaryKey"].ToString() == "YES")
                {
                    hasPK = true;
                    PK_field = column["column_name"].ToString();
                }
            }

            List<string> footScripts = new List<string>();
            footScripts.Add(@"      
-- 需要再打開
-- CREATE DATABASE [" + databaseName + @"];
            ");
            createTableScript.Remove(createTableScript.Length - 3, 2);
            createTableScript.AppendLine("  );");
            createTableScript.AppendLine("END");
            footScripts.Add(createTableScript.ToString());

            //主鍵
            if (hasPK)
            {
                footScripts.Add(@"
USE [" + databaseName + @"];
ALTER TABLE [" + tableName + @"]
ADD CONSTRAINT PK_" + tableName + @" PRIMARY KEY (" + PK_field + @");
                ");
            }

            //註解            
            for (int j = 0, max_j = jd.Rows.Count; j < max_j; j++)
            {
                string column_name = jd.Rows[j]["column_name"].ToString();
                string comment = jd.Rows[j]["comments"].ToString();
                if (comment != "")
                {
                    createTableScript = new StringBuilder();
                    createTableScript.AppendLine("USE [" + databaseName + "];");
                    createTableScript.AppendLine("EXEC sys.sp_addextendedproperty");
                    createTableScript.AppendLine("@name = N'MS_Description', ");
                    createTableScript.AppendLine("@value = N'" + comment.Replace("'", "''") + "', ");
                    createTableScript.AppendLine("@level0type = N'SCHEMA', @level0name = 'dbo', ");
                    createTableScript.AppendLine("@level1type = N'TABLE', @level1name = '" + tableName + "', ");
                    createTableScript.AppendLine("@level2type = N'COLUMN', @level2name = '" + column_name + "'; ");
                    footScripts.Add(createTableScript.ToString());
                }
            } //註解

            //索引
            var INDEX_fields = mssql_get_table_indexs(databaseName, tableName);
            if (INDEX_fields != null)
            {
                //有 INDEX
                foreach (string k in INDEX_fields.Keys)
                {
                    SQL = @"
                        USE [" + databaseName + @"];
                        CREATE INDEX [" + k + @"]
                        ON [" + tableName + @"] (
                          [" + implode("],[", INDEX_fields[k]) + @"]
                        );
                    ";
                    footScripts.Add(SQL);
                }
            }


            return implode("\n", footScripts);

        } // function end
    }
}
