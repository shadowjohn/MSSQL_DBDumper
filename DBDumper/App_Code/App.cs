using DBDumper;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace App
{
    public class myApp
    {
        public bool run_step1_check_mssql_sqlite_rows()
        {
            //讀出 Mssql、SQLite 筆數是否相同
            bool check = false;
            string SQL = @"
                SELECT [row_count] AS [COUNTER]
                FROM sys.dm_db_partition_stats
                WHERE 
                    [object_id] = OBJECT_ID(@tableName)
            ";
            long st = Convert.ToInt64(Program.my.time());
            Console.Write("MSSQL " + Program.table + " 開始計算筆數...");
            var PA = new Dictionary<string, string>();
            PA["tableName"] = Program.table;
            var ra_mssql = Program.my.selectSQL_SAFE(SQL, PA);
            long t = Convert.ToInt64(Program.my.time()) - st;
            Console.WriteLine("費時：" + t + " 秒");
            Console.WriteLine("MSSQL 筆數：" + Program.my.my_money_format(Convert.ToDouble(ra_mssql.Rows[0]["COUNTER"].ToString()), 3) + " 筆");
            Console.Write("SQLite " + Program.table + " 開始計算筆數...");
            
            SQL = @" SELECT COUNT(1) AS `COUNTER` FROM `" + Program.table + @"` ";

            st = Convert.ToInt64(Program.my.time());
            var ra_sqlite = Program.my.SQLite_selectSQL_SAFE(Program.SQLitePath, SQL, new Dictionary<string, string>());
            t = Convert.ToInt64(Program.my.time()) - st;
            Console.WriteLine("費時：" + t + " 秒");
            Console.WriteLine("SQLite 筆數：" + Program.my.my_money_format(Convert.ToDouble(ra_sqlite.Rows[0]["COUNTER"].ToString()), 3) + " 筆");
            return (ra_mssql.Rows[0]["COUNTER"].ToString() == ra_sqlite.Rows[0]["COUNTER"].ToString());
        } // 比對 MSSQL、SQLite 建置所有的 PK 
        private void run_mssql_to_sqlite_ids(List<string> ids)
        {
            string PK_field = Program.my.mssql_get_table_pk(Program.table);
            List<string> qs = new List<string>();
            var PA = new Dictionary<string, string>();
            //List<string> SQLs = new List<string>();
            for (int i = 0, max_i = ids.Count(); i < max_i; i++)
            {
                qs.Add("@id_" + i.ToString());
                PA["id_" + i.ToString()] = ids[i];
                //SQLs.Add(@"SELECT [" + Program.my.implode("],[", Program.mssql_table_columns) + @"] FROM [" + Program.table + @"] WHERE [" + PK_field + @"] = @id_" + i.ToString() + " ");
            }
            //string SQL = Program.my.implode(" UNION ", SQLs);            
            string SQL = @"
                SELECT  
                    [" + Program.my.implode("],[", Program.mssql_table_columns) + @"] 
                FROM 
                    [" + Program.table + @"] 
                WHERE 
                    [" + PK_field + @"] BETWEEN @s AND @e
            ";
            string SQL_APPEND = "";
            if (Program.filter_SQL != "")
            {
                SQL_APPEND = " AND " + Program.filter_SQL;
            }
            SQL += SQL_APPEND;

            PA = new Dictionary<string, string>();
            PA["s"] = ids[0];
            PA["e"] = ids[ids.Count() - 1];

            //Console.WriteLine(SQL);
            //Console.WriteLine(Program.my.json_encode(PA));
            var ra = Program.my.selectSQL_SAFE(SQL, PA);
            List<string> keys = new List<string>();
            List<string> qss = new List<string>();
            for (int k = 0, max_k = ra.Columns.Count; k < max_k; k++)
            {
                keys.Add(ra.Columns[k].ColumnName);
                qss.Add("@" + ra.Columns[k].ColumnName);
            }
            using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + Program.SQLitePath + ";Version=3;"))
            {
                connection.Open();
                using (SQLiteTransaction transaction = connection.BeginTransaction())
                {
                    using (SQLiteCommand command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText = "INSERT OR IGNORE INTO `" + Program.table + @"` (`";
                        command.CommandText += Program.my.implode("`,`", keys) + "`)VALUES(" + Program.my.implode(",", qss) + ")";
                        for (int i = 0, max_i = ra.Rows.Count; i < max_i; i++)
                        {
                            command.Parameters.Clear();
                            for (int j = 0, max_j = ra.Columns.Count; j < max_j; j++)
                            {
                                string fieldName = ra.Columns[j].ColumnName;
                                command.Parameters.AddWithValue("@" + fieldName, ra.Rows[i][fieldName].ToString());
                            }
                            command.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                }
            }
        } //有 主鍵的查詢寫入方法
        private void run_mssql_to_sqlite_index(List<string> fields, long offset_i)
        {
            //有 index 的查詢寫入方法
            // input: fields (index columns)
            // i: offset start
            //從 1 開始

            string SQL = @"
                SELECT 
                    [" + Program.my.implode("],[", Program.mssql_table_columns) + @"] 
                FROM
                    [" + Program.table + @"]
                ";
            string SQL_APPEND = "";
            if (Program.filter_SQL != "")
            {
                SQL_APPEND = " WHERE " + Program.filter_SQL;
            }
            SQL += @"
                ORDER BY
                    [" + Program.my.implode("] ASC,[", fields) + @"] ASC
                OFFSET " + offset_i.ToString() + @" ROWS
                FETCH NEXT " + Program.run_rows.ToString() + @" ROWS ONLY;
            ";
            var ra = Program.my.selectSQL_SAFE(SQL, new Dictionary<string, string>());
            List<string> keys = new List<string>();
            List<string> qss = new List<string>();
            for (int k = 0, max_k = ra.Columns.Count; k < max_k; k++)
            {
                keys.Add(ra.Columns[k].ColumnName);
                qss.Add("@" + ra.Columns[k].ColumnName);
            }
            using (SQLiteConnection connection = new SQLiteConnection("Data Source=" + Program.SQLitePath + ";Version=3;"))
            {
                connection.Open();
                using (SQLiteTransaction transaction = connection.BeginTransaction())
                {
                    using (SQLiteCommand command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText = "INSERT OR IGNORE INTO `" + Program.table + @"` (`";
                        command.CommandText += Program.my.implode("`,`", keys) + "`)VALUES(" + Program.my.implode(",", qss) + ")";
                        for (int i = 0, max_i = ra.Rows.Count; i < max_i; i++)
                        {
                            command.Parameters.Clear();
                            for (int j = 0, max_j = ra.Columns.Count; j < max_j; j++)
                            {
                                string fieldName = ra.Columns[j].ColumnName;
                                command.Parameters.AddWithValue("@" + fieldName, ra.Rows[i][fieldName].ToString());
                            }
                            command.ExecuteNonQuery();
                        }
                        transaction.Commit();
                    }
                }
            }
        } //有 index 的查詢寫入方法
        public void run_step2_sqlite_set_ids()
        {
            //取得 PK
            string PK_field = Program.my.mssql_get_table_pk(Program.table);
            Console.WriteLine("PK_field: " + PK_field);
            if (PK_field != null)
            {
                //有主鍵的匯入模式
                run_step2_1(PK_field);
                return;
            }
            // 檢查有沒有 index 
            Dictionary<string, List<string>> INDEX_fields = Program.my.mssql_get_table_indexs(Program.table);
            if (INDEX_fields != null)
            {
                //有 INDEX 的匯入模式
                run_step2_2(INDEX_fields);
                return;
            }
        }
        private void run_step2_1(string PK_field)
        {
            Console.WriteLine("run_step2_1: 有主鍵的匯出模式");
            Console.WriteLine("主鍵: " + PK_field);
            //取得資料間距
            string SQL = @"
                SELECT 
                    MIN([" + PK_field + @"]) AS [MIN_PK],
                    MAX([" + PK_field + @"]) AS [MAX_PK]
                FROM 
                    [" + Program.table + @"]
            ";
            string SQL_APPEND = "";
            if (Program.filter_SQL != "")
            {
                SQL_APPEND = " WHERE " + Program.filter_SQL;
            }
            long st = Convert.ToInt64(Program.my.time());
            var ra_mssql = Program.my.selectSQL_SAFE(SQL);
            long t = Convert.ToInt64(Program.my.time()) - st;
            Console.WriteLine("費時：" + t + " 秒");

            Console.WriteLine("MSSQL " + Program.table + " 取得筆數間距...");
            long min_pk = Convert.ToInt64(ra_mssql.Rows[0]["MIN_PK"].ToString());
            long max_pk = Convert.ToInt64(ra_mssql.Rows[0]["MAX_PK"].ToString());
            Console.WriteLine("MSSQL " + Program.table + " " + PK_field + " 開始筆數：" + min_pk.ToString() + ", 結束筆數：" + max_pk.ToString());

            // run_rows (1000) 筆 commit 一次
            int step = 0;
            List<string> ids = new List<string>();
            long start = 0;
            for (long i = min_pk; i <= max_pk; i++)
            {
                if (step == 0)
                {
                    ids = new List<string>();
                    start = i;
                }
                ids.Add(i.ToString());
                step++;
                if (step == Program.run_rows)
                {
                    step = 0;
                    //read mssql、write sqlite
                    Console.WriteLine("Doing..." + start.ToString() + " ~ " + i.ToString() + " , 總數: " + max_pk.ToString());
                    run_mssql_to_sqlite_ids(ids);
                    ids = new List<string>();
                }
            } //for
            if (ids.Count() > 0)
            {
                step = 0;
                //read mssql、write sqlite
                Console.WriteLine("Doing..." + start.ToString() + " ~ " + ids[ids.Count() - 1].ToString() + " , 總數: " + max_pk.ToString());
                run_mssql_to_sqlite_ids(ids);
                ids = new List<string>();
            }
        }
        private void run_step2_2(Dictionary<string, List<string>> INDEX_fields)
        {
            //使用第一個 INDEX
            KeyValuePair<string, List<string>> firstItem = INDEX_fields.First();
            string key = firstItem.Key; // 取得第一組資料的鍵
            List<string> value = firstItem.Value; // 取得第一組資料的值
            Console.WriteLine("run_step2_2: 有 INDEX 的匯出模式");
            Console.WriteLine("INDEX Name: " + key);
            Console.WriteLine("INDEX Columns: " + Program.my.json_encode(value));

            string SQL = @"
                SELECT [row_count] AS [COUNTER]
                FROM sys.dm_db_partition_stats
                WHERE [object_id] = OBJECT_ID(@tableName)
            ";
            long st = Convert.ToInt64(Program.my.time());
            Console.Write("MSSQL " + Program.table + " 開始計算筆數...");
            var PA = new Dictionary<string, string>();
            PA["tableName"] = Program.table;
            var ra_mssql = Program.my.selectSQL_SAFE(SQL, PA);
            long t = Convert.ToInt64(Program.my.time()) - st;
            Console.WriteLine("費時：" + t + " 秒");
            Console.WriteLine("MSSQL 筆數：" + Program.my.my_money_format(Convert.ToDouble(ra_mssql.Rows[0]["COUNTER"].ToString()), 3) + " 筆");
            for (long i = 0, max_i = Convert.ToInt64(ra_mssql.Rows[0]["COUNTER"].ToString()); i < max_i; i += Program.run_rows)
            {
                long nextI = i + Program.run_rows;
                nextI = (nextI > max_i) ? max_i : nextI;
                Console.WriteLine("Doing... : " + (i + 1).ToString() + " ~ " + nextI.ToString() + " , 總數: " + max_i.ToString());
                run_mssql_to_sqlite_index(value, i);
            }

        }
    }


}
