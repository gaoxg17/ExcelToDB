using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using log4net;
using System.Collections;
using System.Data.SqlClient;
using System.Data.OleDb;
using Oracle.ManagedDataAccess.Client;

namespace ExcelToDB.Lib
{
    public class Exec
    {
        /// <summary>
        /// 私有日志对象
        /// </summary>
        private static readonly ILog logger = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        public static (bool success, string errMsg) CheckConfig(csExcelToDB eo)
        {
            if (eo == null)
                return (false, "配置错误");
            if (eo.DBType != DB.ORACLE && eo.DBType != DB.SQLSERVER)
                return (false, "数据库类型错误");
            if (string.IsNullOrWhiteSpace(eo.ConnStr))
                return (false, "连接字符串错误");
            if (eo.Sheets == null || eo.Sheets.Count == 0)
                return (false, "Sheet节点存在错误");
            if (eo.File.Contains("{today}"))
            {
                eo.File = eo.File.Replace("{today}", $"{DateTime.Now:D}");
            }
            if (!File.Exists(eo.File))
            {
                return (false, $"文件{eo.File}不存在");
            }
            foreach (var sheet in eo.Sheets)
            {
                if (string.IsNullOrWhiteSpace(sheet.SheetName))
                    return (false, "SheetName不能为空");
                if (sheet.Fileds == null || sheet.Fileds.Count == 0)
                    return (false, "Filed节点存在错误");
                foreach (var filed in sheet.Fileds)
                {
                    if (string.IsNullOrEmpty(filed.FldName))
                        return (false, "FldName不能为空");
                    if (string.IsNullOrEmpty(filed.RowName))
                        return (false, "配置错误");
                }
            }
            return (true, null);
        }

        public static (bool success, string errMsg) ToDB(csExcelToDB eo)
        {
            return eo.DBType == DB.ORACLE ? ToOracle(eo) : ToSqlServer(eo);
        } 

        public static (bool success, string errMsg) ToOracle(csExcelToDB eo)
        {
            //get sheetnames
            var getExcelSheets = GetExcelSheets(eo.File);
            if (!getExcelSheets.success)
            {
                return (false, "获取SheetNames失败");
            }
            //get excel data
            var getExcelDs = GetExcelDs(eo, getExcelSheets.sheetNames);
            if (!getExcelDs.success)
            {
                return (false, "获取Excel数据失败");
            }
            //into db
            var execMsg = ToOracle(eo, getExcelSheets.sheetNames, getExcelDs.ds);
            return (true, execMsg);
        }

        private static string ToOracle(csExcelToDB eo, string[] sheetNames, DataSet ds)
        {
            var execMsg = string.Empty;
            try
            {
                logger.Info("开始入库数据");
                OracleHelper.connStr = eo.ConnStr;
                foreach (var sheet in eo.Sheets)
                {
                    var _sheetName = $"{sheet.SheetName}$";
                    if (sheetNames.Contains(_sheetName))
                    {
                        DataTable dt = ds.Tables[_sheetName];
                        string sql = sheet.Sql;
                        int index = 2;
                        foreach (DataRow row in dt.Rows)
                        {
                            try
                            {
                                OracleParameter[] paras = new OracleParameter[sheet.Fileds.Count];
                                for (int i = 0; i < sheet.Fileds.Count; i++)
                                {
                                    var filed = sheet.Fileds[i];
                                    //paras[i] = new OracleParameter(filed.FldName, GetOracleDbType(filed.FldType));
                                    //paras[i].Value = row[filed.RowName] ?? DBNull.Value;
                                    paras[i] = new OracleParameter(filed.FldName, row[filed.RowName] ?? DBNull.Value);
                                }
                                OracleHelper.ExecuteNonQuery(sql, paras);
                                execMsg += $"\nSheet【{sheet.SheetName}】第{index}行执行成功";
                            }
                            catch (Exception ex)
                            {
                                execMsg += $"\nSheet【{sheet.SheetName}】第{index}行执行失败：{ex.Message}";
                            }
                            index++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error("导入数据失败：" + ex.Message);
                execMsg = "导入数据失败：" + ex.Message + "\n";
            }
            return execMsg;
        }

        public static (bool success, string errMsg) ToSqlServer(csExcelToDB eo)
        {
            //get sheetnames
            var getExcelSheets = GetExcelSheets(eo.File);
            if (!getExcelSheets.success)
            {
                return (false, "获取SheetNames失败");
            }
            //get excel data
            var getExcelDs = GetExcelDs(eo, getExcelSheets.sheetNames);
            if (!getExcelDs.success)
            {
                return (false, "获取Excel数据失败");
            }
            //into db
            var execMsg = ToSqlServer(eo, getExcelSheets.sheetNames, getExcelDs.ds);
            return (true, execMsg);
        }

        private static string ToSqlServer(csExcelToDB eo, string[] sheetNames, DataSet ds)
        {
            var execMsg = string.Empty;
            try
            {
                logger.Info("开始入库数据");
                SqlServerHelper.connStr = eo.ConnStr;
                foreach (var sheet in eo.Sheets)
                {
                    var _sheetName = $"{sheet.SheetName}$";
                    if (sheetNames.Contains(_sheetName))
                    {
                        DataTable dt = ds.Tables[_sheetName];
                        string sql = sheet.Sql;
                        int index = 2;
                        foreach (DataRow row in dt.Rows)
                        {
                            try
                            {
                                SqlParameter[] paras = new SqlParameter[sheet.Fileds.Count];
                                for (int i = 0; i < sheet.Fileds.Count; i++)
                                {
                                    var filed = sheet.Fileds[i];
                                    //paras[i] = new OracleParameter(filed.FldName, GetOracleDbType(filed.FldType));
                                    //paras[i].Value = row[filed.RowName] ?? DBNull.Value;
                                    paras[i] = new SqlParameter(filed.FldName, row[filed.RowName] ?? DBNull.Value);
                                }
                                SqlServerHelper.ExecuteNonQuery(sql, paras);
                                execMsg += $"\nSheet【{sheet.SheetName}】第{index}行执行成功";
                            }
                            catch (Exception ex)
                            {
                                execMsg += $"\nSheet【{sheet.SheetName}】第{index}行执行失败：{ex.Message}";
                            }
                            index++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error("导入数据失败：" + ex.Message);
                execMsg = "导入数据失败：" + ex.Message + "\n";
            }
            return execMsg;
        }

        private static (bool success, string[] sheetNames) GetExcelSheets(string file)
        {
            try
            {
                logger.Info("开始获取SheetNames");
                string strConn = "Provider=Microsoft.Ace.OleDb.12.0;" + "data source=" + @file + ";Extended Properties='Excel 12.0; HDR=Yes; IMEX=1'";
                OleDbConnection conn = new OleDbConnection(strConn);
                conn.Open();
                DataTable dt = conn.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, null);
                string[] sheetNames = new string[dt.Rows.Count];
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    sheetNames[i] = dt.Rows[i]["TABLE_NAME"].ToString();
                }
                logger.Info("获取SheetNames成功");
                return (true, sheetNames);
            }
            catch (Exception)
            {
                logger.Error("获取SheetNames失败");
                return (false, null);
            }
        }

        private static (bool success, DataSet ds) GetExcelDs(csExcelToDB eo, string[] sheetNames)
        {
            try
            {
                logger.Info("开始获取Excel数据");
                //string strConn = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + @path + ";" + "Extended Properties=Excel 8.0;";
                string strConn = "Provider=Microsoft.Ace.OleDb.12.0;" + "data source=" + @eo.File + ";Extended Properties='Excel 12.0; HDR=Yes; IMEX=1'";  //HDR=yes第一行作为列名；IMEX=1将所有读入数据看作字符
                OleDbConnection conn = new OleDbConnection(strConn);
                conn.Open();
                OleDbDataAdapter myCommand = null;
                DataSet ds = new DataSet();
                foreach (var sheetName in sheetNames)
                {
                    DataTable dt = new DataTable();
                    string where = string.Empty;
                    var sheet = eo.Sheets.Where(e => (e.SheetName + "$") == sheetName).FirstOrDefault();
                    if(sheet == null) { continue; }
                    sheet.Fileds.ForEach(e => {
                        where += string.IsNullOrEmpty(where) ? $" where [{e.RowName}] is not null" : $" or [{e.RowName}] is not null";
                    });
                    string sql = $@"select * from [{sheetName}] {where}";
                    myCommand = new OleDbDataAdapter(sql, strConn);
                    myCommand.Fill(dt);
                    dt.TableName = sheetName;
                    ds.Tables.Add(dt);
                }
                logger.Info("获取Excel数据成功");
                return (true, ds);
            }
            catch (Exception ex)
            {
                logger.Error("获取Excel数据失败");
                return (false, null);
            }
        }

        

        private static OracleDbType GetOracleDbType(string fldType)
        {
            OracleDbType oracleDbType = OracleDbType.Varchar2;
            switch (fldType.ToLower())
            {
                case "varchar2": oracleDbType = OracleDbType.Varchar2; break;
                case "nvarchar2": oracleDbType = OracleDbType.NVarchar2; break;
                case "binary_double": oracleDbType = OracleDbType.BinaryDouble; break;
                case "binary_float": oracleDbType = OracleDbType.BinaryFloat; break;
                case "char": oracleDbType = OracleDbType.Char; break;
                case "date": oracleDbType = OracleDbType.Date; break;
                case "interval day to second": oracleDbType = OracleDbType.IntervalDS; break;
                case "interval year to month": oracleDbType = OracleDbType.IntervalYM; break;
                case "long": oracleDbType = OracleDbType.Long; break;
                case "long raw": oracleDbType = OracleDbType.LongRaw; break;
                case "number": oracleDbType = OracleDbType.Int32; break;
                case "timestamp with local time zone": oracleDbType = OracleDbType.TimeStampLTZ; break;
                case "timestamp with time zone": oracleDbType = OracleDbType.TimeStampTZ; break;
                case "timestamp": oracleDbType = OracleDbType.TimeStamp; break;
                default: break;
            }
            return oracleDbType;
        }
    }
}
