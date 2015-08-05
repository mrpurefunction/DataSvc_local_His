using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Common;

namespace EnvirPortal_Data
{
    /// <summary>
    /// 获得本地数据--通常按照时间范围来选择
    /// </summary>
    public class LocalData
    {
        /// <summary>
        /// get pi history data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetPIHisData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from PiRecords where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// PI Avg Records
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetPIAvgData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from PiAvgRecords where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// get envir hour average data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetEnvirMonitorData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from EnvirMonitorData where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// get exception group data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetExceptionGroupData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from v_Monitor_Exception_Group where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// get exception rulelog match data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetExceptionRuleLogMatchData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from v_Monitor_Exception_rulelog_match where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// get local group rules data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetGroupRulesData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from t_RulelogS where ");
                sb.Append("timelog>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timelog<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Calib rule value
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetCalibRuleValue(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from CalibRuleValue where ");
                sb.Append("timestamps>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and timestamps<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// calib records
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetCalibRecordsData(DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from CalibRecords where ");
                sb.Append("starttime>='");
                sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("' and starttime<='");
                sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss"));
                sb.Append("'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }
    }
}
