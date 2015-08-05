using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Common;

namespace LocalPIData
{
    /// <summary>
    /// 
    /// </summary>
    public class SQLPart
    {
        /// <summary>
        /// constructor
        /// </summary>
        public SQLPart()
        {
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="pn"></param>
        /// <param name="pv"></param>
        /// <param name="mi"></param>
        /// <param name="pi"></param>
        /// <returns></returns>
        public int AddPiRecord(DateTime ts, string pn, float pv, int mi, int pi)
        {
            try
            {
                if (IsPiRdExisted(ts, pn, mi, pi) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into PiRecords(pname,timestamps,pvalue,updatetime,machineid,plantid) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                    sb.Append(pv.ToString() + ",'");
                    sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                    sb.Append(mi.ToString() + ",");
                    sb.Append(pi.ToString() + ")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="pn"></param>
        /// <param name="mi"></param>
        /// <param name="pi"></param>
        /// <returns></returns>
        public bool? IsPiRdExisted(DateTime ts, string pn, int mi, int pi)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from PiRecords t where ");
                sb.Append("t.pname = '" + pn + "' and ");
                sb.Append("t.timestamps = '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.machineid =" + mi.ToString() + " and ");
                sb.Append("t.plantid=" + pi.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                if ((int)db.ExecuteScalar(dbc) > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <param name="pv"></param>
        /// <param name="mi"></param>
        /// <param name="pi"></param>
        /// <returns></returns>
        public int AddAvgRd(string pn, DateTime ts, double pv, params int[] mi_pi)
        {
            try
            {
                if (IsAvgRdExisted(pn, ts) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into PIAvgRecords(pname,timestamps,pvalue,updatetime) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                    sb.Append(pv.ToString() + ",'");
                    sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                    Database db = DatabaseFactory.CreateDatabase("dbconn");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <param name="mi"></param>
        /// <param name="pi"></param>
        /// <returns></returns>
        public bool? IsAvgRdExisted(string pn, DateTime ts, params int[] mi_pi)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from PIAvgRecords t where ");
                sb.Append("t.pname = '" + pn + "' and ");
                sb.Append("t.timestamps = '" + ts.ToString("yyyy-MM-dd HH:mm:ss")+"'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                if ((int)db.ExecuteScalar(dbc) > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 获取单个PI均值记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <returns></returns>
        public DataSet GetSingleAvgRd(string pn, DateTime ts)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                sb.Append("select * from PIAvgRecords t where ");
                sb.Append("t.pname = '" + pn + "' and ");
                sb.Append("t.timestamps = '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataSet GetPIPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from point_machine_map");
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
        /// 
        /// </summary>
        /// <returns></returns>
        public DataSet GetPIAvgPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from Points_Machines_ShiftSecs");
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
        /// 获得标定点
        /// </summary>
        /// <returns></returns>
        public DataSet GetCalibPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from CalibPoints_Machines_ShiftSecs");
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
        /// 标定时间区间记录是否存在
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public bool? IsCalibSpanRdExisted(string pn, DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from CalibRecords t where ");
                sb.Append("t.pointid = '" + pn + "' and ");
                sb.Append("t.starttime = '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.endtime = '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                if ((int)db.ExecuteScalar(dbc) > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 获得相关联的区间
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="rt"></param>
        /// <returns></returns>
        public DataSet GetRelatedSpans(string pn, DateTime st, DateTime et, RelatedType rt)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                //cst<=hst and cet>=het
                if (rt == RelatedType.OutSide)
                {
                    sb.Append("select * from CalibRecords t where ");
                    sb.Append("t.pointid = '" + pn + "' and ");
                    sb.Append("t.starttime <= '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.endtime >= '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                }
                //cst<=hst and (cet<het and cet>hst)
                else if (rt == RelatedType.ForeIntersect)
                {
                    sb.Append("select * from CalibRecords t where ");
                    sb.Append("t.pointid = '" + pn + "' and ");
                    sb.Append("t.starttime <= '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.endtime < '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.endtime > '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                }
                //(cst>hst and cst<het) and cet>=het
                else if (rt == RelatedType.BackIntersect)
                {
                    sb.Append("select * from CalibRecords t where ");
                    sb.Append("t.pointid = '" + pn + "' and ");
                    sb.Append("t.starttime > '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.starttime < '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.endtime >= '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                }
                //cst>hst and cet<het
                else if (rt == RelatedType.Inside)
                {
                    sb.Append("select * from CalibRecords t where ");
                    sb.Append("t.pointid = '" + pn + "' and ");
                    sb.Append("t.starttime > '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                    sb.Append("t.endtime < '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                }
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
                //return null;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 添加标定时间区间记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public int AddCalibSpanRd(string pn, DateTime st, DateTime et)
        {
            try
            {
                if (IsCalibSpanRdExisted(pn,st,et) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into CalibRecords(pointid,starttime,endtime,updatetime) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(st.ToString("yyyy-MM-dd HH:mm:ss") + "','");
                    sb.Append(et.ToString("yyyy-MM-dd HH:mm:ss") + "','");
                    sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                    Database db = DatabaseFactory.CreateDatabase("dbconn");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        /// <summary>
        /// 标定小时均值记录是否存在
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <returns></returns>
        public bool? IsCalibAvgRdExisted(string pn, DateTime ts)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from CalibRuleValue t where ");
                sb.Append("t.pname = '" + pn + "' and ");
                sb.Append("t.timestamps = '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                if ((int)db.ExecuteScalar(dbc) > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 添加标定小时均值记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <param name="pv"></param>
        /// <returns></returns>
        public int AddCalibAvgRd(string pn, DateTime ts, double pv, int am)
        {
            try
            {
                if (IsCalibAvgRdExisted(pn, ts) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into CalibRuleValue(pname,timestamps,pvalue,actualminutes,updatetime) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                    sb.Append(pv.ToString() + ",");
                    sb.Append(am.ToString() + ",'");
                    sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                    Database db = DatabaseFactory.CreateDatabase("dbconn");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    return 0;
                }
                else
                {
                    return -1;
                }
            }
            catch (Exception ex)
            {
                return -1;
            }
        }

        /// <summary>
        /// 获得单条calib average value记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="ts"></param>
        /// <returns></returns>
        public DataSet GetSingleCalibAvgRd(string pn, DateTime ts)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                sb.Append("select * from CalibRuleValue t where ");
                sb.Append("t.pname = '" + pn + "' and ");
                sb.Append("t.timestamps = '" + ts.ToString("yyyy-MM-dd HH:mm:ss") + "'");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 获取时间段内标定结束记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetCalibEndRds(string pn, DateTime st, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from t_RulelogS t where t.rulename='");
                sb.Append(pn + "' and ");
                sb.Append("t.timelog >= '" + st.ToString("yyyy-MM-dd HH:mm:ss") + "' and ");
                sb.Append("t.timelog <= '" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.AlarmLog='标定结束'");
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
        /// 获取标定结束时间对应的开始记录
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public DataSet GetFirstCalibBeginRd(string pn, DateTime et)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select top 1 * from t_RulelogS t where t.rulename='");
                sb.Append(pn + "' and ");
                sb.Append("t.timelog<'" + et.ToString("yyyy-MM-dd HH:mm:ss") + "' and t.AlarmLog='仪表标定' order by t.timelog desc");
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
