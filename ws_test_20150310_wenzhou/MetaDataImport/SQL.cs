using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Common;
using System.Data;

namespace MetaDataImport
{
    public class SQL
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataSet GetCalibPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from CalibPoints");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }


        public void AddCalibPoint(string pn,int pid,int mid)
        {
            try
            {
                if (IsCalibPointExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into CalibPoints(pointname,plantid,machine) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(mid.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCalibPointExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from CalibPoints t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetChartPointConfig()
        {
            try 
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public void AddChartPointConfig(string pn, int pid, string type, int id)
        {
            try
            {
                if (IsChartPointConfigExisted(pn, pid, type) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig(pointname,plantid,id,type) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString() + ",'");
                    sb.Append(type + "')");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsChartPointConfigExisted(string pn,int pid, string type)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.type = '" + type + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_F_C()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_fgd_calib");
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
        public void AddCPC_F_C(string pn, int pid,int id)
        {
            try
            {
                if (IsCPC_F_CExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_fgd_calib(pointname,plantid,id) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_F_CExisted(string pn,int pid)
        {
            try 
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_fgd_calib t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_F_L()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_fgd_launch");
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
        public void AddCPC_F_L(string pn, int pid, int id)
        {
            try
            {
                if (IsCPC_F_LExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_fgd_launch(pointname,plantid,id) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_F_LExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_fgd_launch t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_F_S()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_fgd_scon");
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
        public void AddCPC_F_S(string pn, int pid, int id, string ud1)
        {
            try
            {
                if (IsCPC_F_SExisted(pn, pid, ud1) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_fgd_scon(pointname,ud1,plantid,id) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ud1.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_F_SExisted(string pn,int pid, string ud1)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_fgd_scon t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.ud1 = '" + ud1 + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_S_C()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_scr_calib");
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
        public void AddCPC_S_C(string pn, int pid, int id)
        {
            try
            {
                if (IsCPC_S_CExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_scr_calib(pointname,plantid,id) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_S_CExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_scr_calib t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_S_L()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_scr_launch");
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
        public void AddCPC_S_L(string pn, int pid, int id, string ud1)
        {
            try
            {
                if (IsCPC_S_LExisted(pn, pid,ud1) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_scr_launch(pointname,ud1,plantid,id) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ud1.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_S_LExisted(string pn,int pid,string ud1)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_scr_launch t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.ud1 = '" + ud1 + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetCPC_S_S()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from chartpointconfig_scr_scon");
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
        public void AddCPC_S_S(string pn, int pid, int id, string ud1)
        {
            try
            {
                if (IsCPC_S_SExisted(pn, pid,ud1) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into chartpointconfig_scr_scon(pointname,ud1,plantid,id) values('");
                    sb.Append(pn.ToString() + "','");
                    sb.Append(ud1.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsCPC_S_SExisted(string pn, int pid, string ud1)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from chartpointconfig_scr_scon t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.ud1 = '" + ud1 + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetEnvirIndicators()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from EnvirIndicator");
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
        public void AddEnvirIndicator(int id)
        {
            try
            {
                if (IsEnvirIndicatorExisted(id) == false)
                {
                    //not existed
                    //StringBuilder sb = new StringBuilder();
                    //sb.Append("insert into PIAvgRecords(pname,timestamps,pvalue,updatetime) values('");
                    //sb.Append(pn.ToString() + "','");
                    //sb.Append(ts.ToString("yyyy-MM-dd HH:mm:ss") + "',");
                    //sb.Append(pv.ToString() + ",'");
                    //sb.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "')");
                    //Database db = DatabaseFactory.CreateDatabase("dbconn");
                    //System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    //db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsEnvirIndicatorExisted(int id)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from EnvirIndicator t where ");
                sb.Append("t.id=" + id.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetHourAvgPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from pihouravgpoints");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public void AddHourAvgPoint(string pn, int pid, int shiftsecs)
        {
            try
            {
                if (IsHourAvgPointExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into pihouravgpoints(pointname,plantid,shiftsecs) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(shiftsecs.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsHourAvgPointExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from pihouravgpoints t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetPMM()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from Point_Machine_Map");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public void AddPMM(string pn, int pid, int id, int mid, int enabled, string description)
        {
            try
            {
                if (IsPMMExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into Point_Machine_Map(pointname,plantid,id,machineid,enabled,description) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString() + ",");
                    sb.Append(mid.ToString() + ",");
                    sb.Append(enabled.ToString() + ",'");
                    sb.Append(description + "')");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsPMMExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from point_machine_map t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetRelevantPoints()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from RelevantPoints");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public void AddRelevantPoint(string pn, int pid, string ptype, int mid)
        {
            try
            {
                if (IsRelevantPointExisted(pn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into RelevantPoints(pointname,plantid,pointtype,machineid) values('");
                    sb.Append(pn.ToString() + "',");
                    sb.Append(pid.ToString() + ",'");
                    sb.Append(ptype + "',");
                    sb.Append(mid.ToString() + ")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsRelevantPointExisted(string pn,int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from relevantpoints t where ");
                sb.Append("t.pointname = '" + pn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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
        /// <returns></returns>
        public DataSet GetRMM()
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select * from Rule_Machine_Map");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                return db.ExecuteDataSet(dbc);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public void AddRMM(string rn, int pid, int id, int mid, int enabled)
        {
            try
            {
                if (IsRMMExisted(rn, pid) == false)
                {
                    //not existed
                    StringBuilder sb = new StringBuilder();
                    sb.Append("insert into rule_machine_map(rulename,plantid,id,machineid,enabled) values('");
                    sb.Append(rn.ToString() + "',");
                    sb.Append(pid.ToString() + ",");
                    sb.Append(id.ToString() + ",");
                    sb.Append(mid.ToString() + ",");
                    sb.Append(enabled.ToString());
                    sb.Append(")");
                    Database db = DatabaseFactory.CreateDatabase("dbconn_r");
                    System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                    db.ExecuteNonQuery(dbc);
                    //return 0;
                }
                else
                {
                    //return -1;
                }
            }
            catch (Exception ex)
            {
                //return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool? IsRMMExisted(string rn, int pid)
        {
            try
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("select count(*) from rule_machine_map t where ");
                sb.Append("t.rulename = '" + rn + "' and ");
                sb.Append("t.plantid=" + pid.ToString());
                Database db = DatabaseFactory.CreateDatabase("dbconn_r");
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

    }
}
