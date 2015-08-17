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
    /// 根据不同要求将数据写入或者更新到数据表中
    /// </summary>
    public class Business
    {
        /// <summary>
        /// 
        /// </summary>
        public static int plantid = 1;

        /// <summary>
        /// 
        /// </summary>
        static cems.cemsSoapClient client = new cems.cemsSoapClient("cemsSoap");

        #region Envir Monitor Data
        /// <summary>
        /// update remote envir monitor data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void UpdateRemoteEnvirMonitorData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetEnvirMonitorData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.UpdateMonitorData_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), dr["enterprise"].ToString(), dr["pointname"].ToString(), int.Parse(dr["indicatorid"].ToString()), double.Parse(dr["indicatorvalue"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {

            }

        }

        /// <summary>
        /// add new envir monitor data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteEnvirMonitorData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetEnvirMonitorData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddMonitorData_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), dr["enterprise"].ToString(), dr["pointname"].ToString(), int.Parse(dr["indicatorid"].ToString()), double.Parse(dr["indicatorvalue"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }
        #endregion

        #region Exception Group Data
        /// <summary>
        /// update remote exception group data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void UpdateRemoteExceptionGroupData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetExceptionGroupData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.UpdateExceptionGroup_c(long.Parse(dr["id"].ToString()), long.Parse(dr["envir_id"].ToString()), int.Parse(dr["typeid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// add new exception group data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteExceptionGroupData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetExceptionGroupData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddExceptionGroup_c(long.Parse(dr["id"].ToString()), long.Parse(dr["envir_id"].ToString()), int.Parse(dr["typeid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }
        #endregion

        #region Exception RuleLog Match Data

        /// <summary>
        /// update remote exception rulelog match data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void UpdateRemoteExceptionRuleLogMatchData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetExceptionRuleLogMatchData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.UpdateExceptionRuleLog_c(long.Parse(dr["id"].ToString()), long.Parse(dr["envir_id"].ToString()), long.Parse(dr["rule_id"].ToString()), int.Parse(dr["typeid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }


        /// <summary>
        /// add new exception rulelog match data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteExceptionRuleLogMatchData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetExceptionRuleLogMatchData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddExceptionRuleLog_c(long.Parse(dr["id"].ToString()), long.Parse(dr["envir_id"].ToString()), long.Parse(dr["rule_id"].ToString()), int.Parse(dr["typeid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }
        #endregion

        #region Group Rules
        /// <summary>
        /// update remote group rules data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void UpdateRemoteGroupRulesData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetGroupRulesData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.UpdateRuleLog_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timelog"].ToString()), dr["rulename"].ToString(), dr["alarmlog"].ToString(), dr["alarmdis"].ToString(), dr["cemstype"].ToString(), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// add new group rules data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteGroupRulesData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetGroupRulesData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddRuleLogRd_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timelog"].ToString()), dr["rulename"].ToString(), dr["alarmlog"].ToString(), dr["alarmdis"].ToString(), dr["cemstype"].ToString(), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }
        #endregion

        #region PI History Data
        /// <summary>
        /// update remote PI history data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void UpdateRemotePIHisData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetPIHisData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.UpdatePiRecord_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), dr["pname"].ToString(), float.Parse(dr["pvalue"].ToString()), int.Parse(dr["machineid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// add new PI history data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemotePIHisData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetPIHisData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddPiRecord_c(long.Parse(dr["id"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), dr["pname"].ToString(), float.Parse(dr["pvalue"].ToString()), int.Parse(dr["machineid"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }
        #endregion

        #region PI Avg Data

        /// <summary>
        /// add new PI Avg data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemotePIAvgData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetPIAvgData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddPiAvgRecord_c(dr["pname"].ToString(), float.Parse(dr["pvalue"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        #endregion

        #region Calib Rds

        /// <summary>
        /// add new Calib Rds data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteCalibRdsData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetCalibRecordsData(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddCalibRecord_c(dr["pointid"].ToString(), DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()), plantid);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        #endregion

        #region Calib Rule Value

        /// <summary>
        /// add new Calib Rds data
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void AddRemoteCalibRuleValueData(DateTime st, DateTime et)
        {
            try
            {
                DataSet ds = (new LocalData()).GetCalibRuleValue(st, et);
                if (ds != null)
                {
                    foreach (DataRow dr in ds.Tables[0].Rows)
                    {
                        client.AddCalibRuleValueRecord_c(dr["pname"].ToString(), float.Parse(dr["pvalue"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), DateTime.Parse(dr["timestamps"].ToString()), plantid, int.Parse(dr["actualminutes"].ToString()));
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        #endregion

    }
}
