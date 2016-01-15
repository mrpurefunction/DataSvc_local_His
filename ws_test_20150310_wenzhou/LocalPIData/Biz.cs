﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;
using System.Configuration;
using System.IO;

using System.Threading.Tasks;

namespace LocalPIData
{
    /// <summary>
    /// 
    /// </summary>
    public enum RelatedType
    {
        OutSide,
        Inside,
        ForeIntersect,
        BackIntersect,
        In_Fore_Back
    }

    /// <summary>
    /// 
    /// </summary>
    public class SingleSpanInfo
    {
        public DateTime st;
        public DateTime et;
        public double? spanavg;
    }

    public class Biz
    {
        /// <summary>
        /// 
        /// </summary>
        //modified 20160108
        public static int plantid = int.Parse((string)(new AppSettingsReader()).GetValue("plantid", typeof(string)));

        //public List<string> output_web_config = null;
        /// <summary>
        /// 
        /// </summary>
        public Biz()
        {
            //if (output_web_config == null)
            //{
            //    output_web_config = new List<string>();
            //}
        }

        /// <summary>
        /// realtime for his value
        /// </summary>
        /// <param name="ts"></param>
        public void RealTimeBiz(DateTime ts)
        {
            DataSet pipoints;
            pipoints = (new SQLPart()).GetPIPoints();
            if (pipoints != null)
            {
                foreach (DataRow dr in pipoints.Tables[0].Rows)
                {
                    double? rv = (new PIHisData()).GetHisValue(dr["pointname"].ToString(), ts);
                    if (rv != null)
                    {
                        (new SQLPart()).AddPiRecord(ts, dr["pointname"].ToString(), (float)rv, int.Parse(dr["machineid"].ToString()), plantid/*int.Parse(dr[""].ToString())*/);
                    }
                }
            }
        }

        /// <summary>
        /// realtime for avg value
        /// </summary>
        /// <param name="ts"></param>
        public void RealTimeBiz_avg(DateTime ts)
        {
            DataSet piavgpoints;
            piavgpoints = (new SQLPart()).GetPIAvgPoints();
            if (piavgpoints != null)
            {
                foreach (DataRow dr in piavgpoints.Tables[0].Rows)
                {
                    double? rv = (new PIAvgData()).GetAvgValue(dr["pointname"].ToString(), ts, ts.AddHours(1), int.Parse(dr["shiftsecs"].ToString()));
                    if (rv != null)
                    {
                        (new SQLPart()).AddAvgRd(dr["pointname"].ToString(), ts, (float)rv /*, int.Parse(dr[""].ToString()), int.Parse(dr[""].ToString())*/);
                    }
                }
            }
        }

        /// <summary>
        /// history of his value
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void HistoryBiz(DateTime st, DateTime et)
        {
            DataSet pipoints;
            DateTime tempdt;
            pipoints = (new SQLPart()).GetPIPoints();
            if (pipoints != null)
            {
                foreach (DataRow dr in pipoints.Tables[0].Rows)
                {
                    tempdt = st;
                    while (tempdt <= et)
                    {
                        double? rv = (new PIHisData()).GetHisValue(dr["pointname"].ToString(), tempdt);
                        if (rv != null)
                        {
                            (new SQLPart()).AddPiRecord(tempdt, dr["pointname"].ToString(), (float)rv, int.Parse(dr["machineid"].ToString()), plantid/*int.Parse(dr[""].ToString())*/);
                        }
                        tempdt = tempdt.AddMinutes(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void HistoryBiz_Remote(DateTime st, DateTime et)
        {
            DataSet pipoints;
            DateTime tempdt;
            pipoints = (new SQLPart()).GetPIPoints();
            if (pipoints != null)
            {
                foreach (DataRow dr in pipoints.Tables[0].Rows)
                {
                    tempdt = st;
                    while (tempdt <= et)
                    {
                        double? rv = (new PIHisData()).GetHisValue(dr["pointname"].ToString(), tempdt);
                        if (rv != null)
                        {
                            (new SQLPart()).AddPiRecord_Remote(tempdt, dr["pointname"].ToString(), (float)rv, int.Parse(dr["machineid"].ToString()), plantid/*int.Parse(dr[""].ToString())*/);
                        }
                        tempdt = tempdt.AddMinutes(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// 历史平均值函数
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void HistoryBiz_avg(DateTime st, DateTime et)
        {
            DataSet piavgpoints;
            DateTime tempdt;
            piavgpoints = (new SQLPart()).GetPIAvgPoints();
            if (piavgpoints != null)
            {
                foreach (DataRow dr in piavgpoints.Tables[0].Rows)
                {
                    tempdt = st;
                    while (tempdt <= et)
                    {
                        double? rv = (new PIAvgData()).GetAvgValue(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), int.Parse(dr["shiftsecs"].ToString()));
                        if (rv != null)
                        {
                            (new SQLPart()).AddAvgRd(dr["pointname"].ToString(), tempdt, (float)rv /*, int.Parse(dr[""].ToString()), int.Parse(dr[""].ToString())*/);
                        }
                        tempdt = tempdt.AddHours(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// 标定时间区间业务
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibSpanBiz(DateTime st, DateTime et)
        {
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            if (calibpoints != null)
            {
                DataSet calibends;
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    calibends = (new SQLPart()).GetCalibEndRds(dr["pointname"].ToString(), st, et);
                    if (calibends != null)
                    {
                        DataSet firstbegin;
                        foreach (DataRow dr2 in calibends.Tables[0].Rows)
                        {
                            firstbegin = (new SQLPart()).GetFirstCalibBeginRd(dr2["rulename"].ToString(), DateTime.Parse(dr2["timelog"].ToString()));
                            if (firstbegin != null)
                            {
                                foreach (DataRow dr3 in firstbegin.Tables[0].Rows)
                                {
                                    (new SQLPart()).AddCalibSpanRd(dr2["rulename"].ToString(), DateTime.Parse(dr3["timelog"].ToString()), DateTime.Parse(dr2["timelog"].ToString()));
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 标定时间区间业务, sync mode
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibSpanBiz_Sync(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            if (calibpoints != null)
            {
                DataSet calibends;
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    calibends = (new SQLPart()).GetCalibEndRds(dr["pointname"].ToString(), st, et);
                    if (calibends != null)
                    {
                        DataSet firstbegin;
                        foreach (DataRow dr2 in calibends.Tables[0].Rows)
                        {
                            firstbegin = (new SQLPart()).GetFirstCalibBeginRd(dr2["rulename"].ToString(), DateTime.Parse(dr2["timelog"].ToString()));
                            if (firstbegin != null)
                            {
                                foreach (DataRow dr3 in firstbegin.Tables[0].Rows)
                                {
                                    //(new SQLPart()).AddCalibSpanRd(dr2["rulename"].ToString(), DateTime.Parse(dr3["timelog"].ToString()), DateTime.Parse(dr2["timelog"].ToString()));
                                    ce.AddToCrl2(dr2["rulename"].ToString(), DateTime.Parse(dr3["timelog"].ToString()), DateTime.Parse(dr2["timelog"].ToString()));
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 标定规则值业务
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibRuleValueBiz(DateTime st, DateTime et)
        {         
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            DateTime tempdt;
            if (calibpoints != null)
            {
                //biz for foreintersect,backintersect,inside
                DataSet calibspans;
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    tempdt = st;
                    List<SingleSpanInfo> ssis = new List<SingleSpanInfo>();
                    while (tempdt < et)
                    {
                        ssis.Clear();
                        SingleSpanInfo ssi = new SingleSpanInfo() { st = tempdt, et = tempdt.AddHours(1.0), spanavg = null };
                        ssis.Add(ssi);

                        //Outside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.OutSide);
                        if (calibspans != null)
                        {
                            if (calibspans.Tables[0].Rows.Count > 0)
                            {
                                tempdt = tempdt.AddHours(1);
                                continue;
                            }
                        }

                        //Fore
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.ForeIntersect);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果span的结束时间>ssis[0]的开始时间,则用此结束时间替换ssis[0]的开始时间
                                if (DateTime.Parse(dr2["endtime"].ToString()) > ssis[0].st)
                                {
                                    ssis[0].st = DateTime.Parse(dr2["endtime"].ToString());
                                }
                            }
                        }

                        //Back
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.BackIntersect);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果span的开始时间<ssis[0]的结束时间,则用此开始时间替换ssis[0]的结束时间
                                if (DateTime.Parse(dr2["starttime"].ToString()) < ssis[0].et)
                                {
                                    ssis[0].et = DateTime.Parse(dr2["starttime"].ToString());
                                }
                            }
                        }

                        //Inside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.Inside);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                foreach (SingleSpanInfo s in ssis)
                                {
                                    // 查看s的时间区间是否覆盖span区间，如果是，则调整当前区间，并加入新的区间
                                    if ((DateTime.Parse(dr2["starttime"].ToString()) >= s.st) && (DateTime.Parse(dr2["endtime"].ToString()) <= s.et))
                                    {
                                        DateTime temp = s.et;
                                        s.et = DateTime.Parse(dr2["starttime"].ToString());
                                        SingleSpanInfo ssin = new SingleSpanInfo() { st = DateTime.Parse(dr2["endtime"].ToString()), et = temp, spanavg = null };
                                        ssis.Add(ssin);
                                        break;
                                    }
                                    if ((DateTime.Parse(dr2["starttime"].ToString()) <= s.st) && (DateTime.Parse(dr2["endtime"].ToString()) > s.st))
                                    {
                                        s.st = DateTime.Parse(dr2["endtime"].ToString());
                                    }
                                }
                            }
                        }
                        //从PI中获取相应的均值
                        int totalmins = 0;
                        double avg = 0;                      
                        //如果没有span存在
                        if ((ssis.Count == 1) && (ssis[0].st == tempdt) && (ssis[0].et == tempdt.AddHours(1.0)))
                        {
                            //do nothing
                        }
                        //add rd into calibavg table
                        else
                        {
                            foreach (SingleSpanInfo s in ssis)
                            {
                                totalmins += (int)(s.et - s.st).TotalMinutes;
                                //s.spanavg = (new PI.PIFunc2(null, null, null)).GetAverageValue(dr["pointname"].ToString(), s.st, s.et);
                                s.spanavg = (new PIAvgData()).GetAvgValue(dr["pointname"].ToString(), s.st, s.et, 0/*int.Parse(dr["shiftsecs"].ToString())*/);
                            }
                            //计算该小时的均值
                            if (totalmins != 0)
                            {
                                foreach (SingleSpanInfo s in ssis)
                                {
                                    if (s.spanavg != null)
                                    {
                                        avg += (double)s.spanavg * ((s.et - s.st).TotalMinutes / totalmins);
                                    }
                                }
                            }
                            (new SQLPart()).AddCalibAvgRd(dr["pointname"].ToString(), tempdt, avg, totalmins);
                        }
                        tempdt = tempdt.AddHours(1.0);
                    }
                }              
            }
        }

        /// <summary>
        /// 标定规则值业务, sync mode
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibRuleValueBiz_Sync(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            DateTime tempdt;
            if (calibpoints != null)
            {
                //biz for foreintersect,backintersect,inside
                DataSet calibspans;
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    tempdt = st;
                    List<SingleSpanInfo> ssis = new List<SingleSpanInfo>();
                    while (tempdt < et)
                    {
                        ssis.Clear();
                        SingleSpanInfo ssi = new SingleSpanInfo() { st = tempdt, et = tempdt.AddHours(1.0), spanavg = null };
                        ssis.Add(ssi);

                        //Outside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.OutSide);
                        if (calibspans != null)
                        {
                            if (calibspans.Tables[0].Rows.Count > 0)
                            {
                                tempdt = tempdt.AddHours(1);
                                continue;
                            }
                        }

                        //Fore
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.ForeIntersect);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果span的结束时间>ssis[0]的开始时间,则用此结束时间替换ssis[0]的开始时间
                                if (DateTime.Parse(dr2["endtime"].ToString()) > ssis[0].st)
                                {
                                    ssis[0].st = DateTime.Parse(dr2["endtime"].ToString());
                                }
                            }
                        }

                        //Back
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.BackIntersect);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果span的开始时间<ssis[0]的结束时间,则用此开始时间替换ssis[0]的结束时间
                                if (DateTime.Parse(dr2["starttime"].ToString()) < ssis[0].et)
                                {
                                    ssis[0].et = DateTime.Parse(dr2["starttime"].ToString());
                                }
                            }
                        }

                        //Inside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.Inside);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                foreach (SingleSpanInfo s in ssis)
                                {
                                    // 查看s的时间区间是否覆盖span区间，如果是，则调整当前区间，并加入新的区间
                                    if ((DateTime.Parse(dr2["starttime"].ToString()) >= s.st) && (DateTime.Parse(dr2["endtime"].ToString()) <= s.et))
                                    {
                                        DateTime temp = s.et;
                                        s.et = DateTime.Parse(dr2["starttime"].ToString());
                                        SingleSpanInfo ssin = new SingleSpanInfo() { st = DateTime.Parse(dr2["endtime"].ToString()), et = temp, spanavg = null };
                                        ssis.Add(ssin);
                                        break;
                                    }
                                    if ((DateTime.Parse(dr2["starttime"].ToString()) <= s.st) && (DateTime.Parse(dr2["endtime"].ToString()) > s.st))
                                    {
                                        s.st = DateTime.Parse(dr2["endtime"].ToString());
                                    }
                                }
                            }
                        }
                        //从PI中获取相应的均值
                        int totalmins = 0;
                        double avg = 0;
                        //如果没有span存在
                        if ((ssis.Count == 1) && (ssis[0].st == tempdt) && (ssis[0].et == tempdt.AddHours(1.0)))
                        {
                            //do nothing
                        }
                        //add rd into calibavg table
                        else
                        {
                            foreach (SingleSpanInfo s in ssis)
                            {
                                totalmins += (int)(s.et - s.st).TotalMinutes;
                                //s.spanavg = (new PI.PIFunc2(null, null, null)).GetAverageValue(dr["pointname"].ToString(), s.st, s.et);
                                s.spanavg = (new PIAvgData()).GetAvgValue(dr["pointname"].ToString(), s.st, s.et, 0/*int.Parse(dr["shiftsecs"].ToString())*/);
                            }
                            //计算该小时的均值
                            if (totalmins != 0)
                            {
                                foreach (SingleSpanInfo s in ssis)
                                {
                                    if (s.spanavg != null)
                                    {
                                        avg += (double)s.spanavg * ((s.et - s.st).TotalMinutes / totalmins);
                                    }
                                }
                            }
                            //(new SQLPart()).AddCalibAvgRd(dr["pointname"].ToString(), tempdt, avg, totalmins);
                            ce.AddToCrvl2(dr["pointname"].ToString(), tempdt, avg, totalmins);
                        }
                        tempdt = tempdt.AddHours(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// 标定规则值业务--outside span情形
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibRuleValueBiz_Outside(DateTime st, DateTime et)
        {
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            DateTime tempdt;
            if (calibpoints != null)
            {
                DataSet calibspans;
                //biz for outside
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    tempdt = st.AddHours(-3.0);
                    List<SingleSpanInfo> ssis = new List<SingleSpanInfo>();
                    while (tempdt < et.AddHours(-3.0))
                    {
                        ssis.Clear();
                        //Outside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.OutSide);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果标定开始为整点时间，则需要向前推一个小时
                                if ((DateTime.Parse(dr2["starttime"].ToString()).Minute == 0) && (DateTime.Parse(dr2["starttime"].ToString()).Second == 0))
                                {
                                    //starttime 需要整点时间
                                    SingleSpanInfo ssi = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(-1.0), et = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(-1.0).AddHours(1.0), spanavg = null };
                                    ssis.Add(ssi);
                                }
                                else
                                {
                                    //starttime 需要整点时间
                                    SingleSpanInfo ssi = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")), et = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(1.0), spanavg = null };
                                    ssis.Add(ssi);
                                }
                                //endtime 需要整点时间
                                SingleSpanInfo ssi2 = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["endtime"].ToString()).ToString("yyyy-MM-dd HH:00:00")), et = DateTime.Parse(DateTime.Parse(dr2["endtime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(1.0), spanavg = null };
                                ssis.Add(ssi2);
                            }

                            bool canbecalculated = false;
                            foreach (SingleSpanInfo s in ssis)
                            {
                                if ((new SQLPart()).IsCalibAvgRdExisted(dr["pointname"].ToString(), s.st) == true)
                                {
                                    //canbecalculated = true;
                                    DataSet sav = (new SQLPart()).GetSingleCalibAvgRd(dr["pointname"].ToString(), s.st);
                                    if (sav != null)
                                    {
                                        foreach (DataRow dr5 in sav.Tables[0].Rows)
                                        {
                                            s.et = s.st.AddMinutes(int.Parse(dr5["actualminutes"].ToString()));
                                            s.spanavg = double.Parse(dr5["pvalue"].ToString());
                                        }
                                        canbecalculated = true;
                                    }
                                }
                                else if ((new SQLPart()).IsAvgRdExisted(dr["pointname"].ToString(), s.st) == true)
                                {
                                    //canbecalculated = true;
                                    DataSet sav = (new SQLPart()).GetSingleAvgRd(dr["pointname"].ToString(), s.st);
                                    if (sav != null)
                                    {
                                        foreach (DataRow dr5 in sav.Tables[0].Rows)
                                        {
                                            s.et = s.st.AddHours(1.0);
                                            s.spanavg = double.Parse(dr5["pvalue"].ToString());
                                        }
                                        canbecalculated = true;
                                    }
                                }
                                else
                                {
                                    canbecalculated = false;
                                    break;
                                }
                            }
                            //如果可被计算
                            if (canbecalculated == true)
                            {
                                if ((ssis[0].spanavg != null) || (ssis[1].spanavg != null))
                                {
                                    //add rd into calibavg table
                                    (new SQLPart()).AddCalibAvgRd(dr["pointname"].ToString(), tempdt, (double)(ssis[0].spanavg + ssis[1].spanavg) / 2, 0);
                                }
                            }
                        }
                        tempdt = tempdt.AddHours(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// 标定规则值业务--outside span情形, sync mode
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void CalibRuleValueBiz_Outside_Sync(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet calibpoints;
            calibpoints = (new SQLPart()).GetCalibPoints();
            DateTime tempdt;
            if (calibpoints != null)
            {
                DataSet calibspans;
                //biz for outside
                foreach (DataRow dr in calibpoints.Tables[0].Rows)
                {
                    tempdt = st.AddHours(-3.0);
                    List<SingleSpanInfo> ssis = new List<SingleSpanInfo>();
                    while (tempdt < et.AddHours(-3.0))
                    {
                        ssis.Clear();
                        //Outside
                        calibspans = (new SQLPart()).GetRelatedSpans(dr["pointname"].ToString(), tempdt, tempdt.AddHours(1.0), RelatedType.OutSide);
                        if (calibspans != null)
                        {
                            foreach (DataRow dr2 in calibspans.Tables[0].Rows)
                            {
                                //如果标定开始为整点时间，则需要向前推一个小时
                                if ((DateTime.Parse(dr2["starttime"].ToString()).Minute == 0) && (DateTime.Parse(dr2["starttime"].ToString()).Second == 0))
                                {
                                    //starttime 需要整点时间
                                    SingleSpanInfo ssi = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(-1.0), et = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(-1.0).AddHours(1.0), spanavg = null };
                                    ssis.Add(ssi);
                                }
                                else
                                {
                                    //starttime 需要整点时间
                                    SingleSpanInfo ssi = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")), et = DateTime.Parse(DateTime.Parse(dr2["starttime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(1.0), spanavg = null };
                                    ssis.Add(ssi);
                                }
                                //endtime 需要整点时间
                                SingleSpanInfo ssi2 = new SingleSpanInfo() { st = DateTime.Parse(DateTime.Parse(dr2["endtime"].ToString()).ToString("yyyy-MM-dd HH:00:00")), et = DateTime.Parse(DateTime.Parse(dr2["endtime"].ToString()).ToString("yyyy-MM-dd HH:00:00")).AddHours(1.0), spanavg = null };
                                ssis.Add(ssi2);
                            }

                            bool canbecalculated = false;
                            foreach (SingleSpanInfo s in ssis)
                            {
                                if ((new SQLPart()).IsCalibAvgRdExisted(dr["pointname"].ToString(), s.st) == true)
                                {
                                    //canbecalculated = true;
                                    DataSet sav = (new SQLPart()).GetSingleCalibAvgRd(dr["pointname"].ToString(), s.st);
                                    if (sav != null)
                                    {
                                        foreach (DataRow dr5 in sav.Tables[0].Rows)
                                        {
                                            s.et = s.st.AddMinutes(int.Parse(dr5["actualminutes"].ToString()));
                                            s.spanavg = double.Parse(dr5["pvalue"].ToString());
                                        }
                                        canbecalculated = true;
                                    }
                                }
                                else if ((new SQLPart()).IsAvgRdExisted(dr["pointname"].ToString(), s.st) == true)
                                {
                                    //canbecalculated = true;
                                    DataSet sav = (new SQLPart()).GetSingleAvgRd(dr["pointname"].ToString(), s.st);
                                    if (sav != null)
                                    {
                                        foreach (DataRow dr5 in sav.Tables[0].Rows)
                                        {
                                            s.et = s.st.AddHours(1.0);
                                            s.spanavg = double.Parse(dr5["pvalue"].ToString());
                                        }
                                        canbecalculated = true;
                                    }
                                }
                                else
                                {
                                    canbecalculated = false;
                                    break;
                                }
                            }
                            //如果可被计算
                            if (canbecalculated == true)
                            {
                                if ((ssis[0].spanavg != null) || (ssis[1].spanavg != null))
                                {
                                    //add rd into calibavg table
                                    //(new SQLPart()).AddCalibAvgRd(dr["pointname"].ToString(), tempdt, (double)(ssis[0].spanavg + ssis[1].spanavg) / 2, 0);
                                    ce.AddToCrvl2(dr["pointname"].ToString(), tempdt, (double)(ssis[0].spanavg + ssis[1].spanavg) / 2, 0);
                                }
                            }
                        }
                        tempdt = tempdt.AddHours(1.0);
                    }
                }
            }
        }

        /// <summary>
        /// async running
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void RunningAsync_Month(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            //get raw data from  SCR_StartStop_Outlet_async then deal with these rds
            //[machineid],[pointname],[ts1],[ts2],[scrgrouptype],[machinegrouptype],[src]
            DataSet asyncrd = (new SQLPart()).GetRelatedAsyncRd(st, et);
            if (asyncrd != null)
            {
                foreach (DataRow dr in asyncrd.Tables[0].Rows)
                {
                    //find appropriate rds
                    if (!((int.Parse(dr["src"].ToString()) == 2) && (dr["machinegrouptype"].ToString() == "")))
                    {
                        //Get span for monthly biz
                        List<StartEndPair> sepl = GetStartEndPair_Month(DateTime.Parse(dr["ts1"].ToString()), DateTime.Parse(dr["ts2"].ToString()));
                        if (sepl != null)
                        {
                            foreach (StartEndPair sep in sepl)
                            {
                                //if the span is between the seach area
                                if ((sep.StartTime <= et) && (sep.StartTime >= st))
                                {
                                    if ((dr["scrgrouptype"].ToString() == "") && (sep == sepl.Last()) && (int.Parse(dr["src"].ToString()) == 3))
                                    {
                                        continue;
                                    }
                                    ce.AddToAscrl(dr["pointname"].ToString(), sep.StartTime, sep.EndTime);
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// machine stop span month statistic
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void MachineStopStatistic_Month(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            //get raw data from  machine_startstop then deal with these rds
            //[pointname],[starttime],[endtime],[grouptype]

            DataSet mstoprd = (new SQLPart()).GetRelatedMachineStopRd(st, et);
            if (mstoprd != null)
            {
                foreach (DataRow dr in mstoprd.Tables[0].Rows)
                {
                    //Get span for monthly biz
                    List<StartEndPair> sepl = GetStartEndPair_Month(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            //if the span is between the seach area
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                //if it has undetermined condition, jump over the last rd
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                ce.AddToMsl(dr["pointname"].ToString(), sep.StartTime, sep.EndTime);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// hour avg value calculation--month
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        public void HourAvgValue_Month(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            //get raw data from machine_startstop_reversed for start stop span then deal with these rds
            //[machineid],[starttime],[endtime],[grouptype]
            //with these rds, get pi avg value
            DataSet mrunningrd = (new SQLPart()).GetRelatedMachineRunningRd(st, et);
            if (mrunningrd != null)
            {
                foreach (DataRow dr in mrunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Month(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                DataSet pointsofmachine = (new SQLPart()).GetPointsOfMachine_avg(int.Parse(dr["machineid"].ToString()));
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, int.Parse(dr2["shiftsecs"].ToString()));
                                        //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                        if (avgvalue == null)
                                        {
                                            if (ce.hal_dst.Where(i => i.pointname == dr2["pointname"].ToString() && i.starttime == sep.StartTime && i.endtime == sep.EndTime).Count() > 0)
                                            {
                                                ce.hal_dst.RemoveAll(i => i.pointname == dr2["pointname"].ToString() && i.starttime == sep.StartTime && i.endtime == sep.EndTime);
                                            }
                                        }
                                        else
                                        {
                                            ce.AddToHal(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, (double)avgvalue);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        public void HourAvgValue_ForFurnace(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                DataSet pointsofmachine = (new SQLPart()).GetPIAvgPoints();
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        if (int.Parse(dr2["machineid"].ToString()) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, 0);
                                            //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                            if (avgvalue == null)
                                            {
                                                if (ce.par_ot_dst.Where(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                {
                                                    ce.par_ot_dst.RemoveAll(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                }
                                            }
                                            else
                                            {
                                                ce.AddToPar_otls(dr2["pointname"].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        public void HourAvgValue_ForFurnace_Parallel(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DateTime[] dt_a = EPASync.CocurrencyT.GetAppropriateTimeArray(st, et);
            HourAvgValue_ForFurnace_1(dt_a[0], dt_a[1], ce);
            HourAvgValue_ForFurnace_2(dt_a[2], dt_a[3], ce);
            HourAvgValue_ForFurnace_3(dt_a[4], dt_a[5], ce);
            HourAvgValue_ForFurnace_4(dt_a[6], dt_a[7], ce);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_1(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                DataSet pointsofmachine = (new SQLPart()).GetPIAvgPoints();
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        if (int.Parse(dr2["machineid"].ToString()) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, 0);
                                            //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                            if (avgvalue == null)
                                            {
                                                if (ce.par_ot_dst_1.Where(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                {
                                                    ce.par_ot_dst_1.RemoveAll(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                }
                                            }
                                            else
                                            {
                                                ce.AddToPar_otls_1(dr2["pointname"].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_2(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                DataSet pointsofmachine = (new SQLPart()).GetPIAvgPoints();
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        if (int.Parse(dr2["machineid"].ToString()) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, 0);
                                            //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                            if (avgvalue == null)
                                            {
                                                if (ce.par_ot_dst_2.Where(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                {
                                                    ce.par_ot_dst_2.RemoveAll(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                }
                                            }
                                            else
                                            {
                                                ce.AddToPar_otls_2(dr2["pointname"].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_3(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                DataSet pointsofmachine = (new SQLPart()).GetPIAvgPoints();
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        if (int.Parse(dr2["machineid"].ToString()) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, 0);
                                            //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                            if (avgvalue == null)
                                            {
                                                if (ce.par_ot_dst_3.Where(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                {
                                                    ce.par_ot_dst_3.RemoveAll(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                }
                                            }
                                            else
                                            {
                                                ce.AddToPar_otls_3(dr2["pointname"].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_4(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }
                                DataSet pointsofmachine = (new SQLPart()).GetPIAvgPoints();
                                if (pointsofmachine != null)
                                {
                                    foreach (DataRow dr2 in pointsofmachine.Tables[0].Rows)
                                    {
                                        if (int.Parse(dr2["machineid"].ToString()) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            double? avgvalue = (new PIAvgData()).GetAvgValue(dr2["pointname"].ToString(), sep.StartTime, sep.EndTime, 0);
                                            //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                            if (avgvalue == null)
                                            {
                                                if (ce.par_ot_dst_4.Where(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                {
                                                    ce.par_ot_dst_4.RemoveAll(i => i.pname == dr2["pointname"].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                }
                                            }
                                            else
                                            {
                                                ce.AddToPar_otls_4(dr2["pointname"].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        public void HourAvgValue_ForFurnace_Web(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            List<string> output_web_config = LoadOutputWebConfig();
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                if ((output_web_config != null) && (output_web_config.Count > 0))
                                {
                                    foreach (string c in output_web_config)
                                    {
                                        if (int.Parse(c.Split(',')[1]) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            DataSet ds = (new SQLPart()).GetSingleAvgRd_Web(c.Split(',')[1], c.Split(',')[2], DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                            if (ds != null && ds.Tables[0].Rows.Count > 0)
                                            {
                                                double? avgvalue = double.Parse(ds.Tables[0].Rows[0][0].ToString());
                                                //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                                if (avgvalue == null)
                                                {
                                                    if (ce.par_otw_dst.Where(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                    {
                                                        ce.par_otw_dst.RemoveAll(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                    }
                                                }
                                                else
                                                {
                                                    ce.AddToPar_otwls(c.Split(',')[0].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        public void HourAvgValue_ForFurnace_Web_Parallel(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DateTime[] dt_a = EPASync.CocurrencyT.GetAppropriateTimeArray(st, et);
            Parallel.Invoke(
                () => HourAvgValue_ForFurnace_Web_1(dt_a[0], dt_a[1], ce),
                () => HourAvgValue_ForFurnace_Web_2(dt_a[2], dt_a[3], ce),
                () => HourAvgValue_ForFurnace_Web_3(dt_a[4], dt_a[5], ce),
                () => HourAvgValue_ForFurnace_Web_4(dt_a[6], dt_a[7], ce)
                );
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_Web_1(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            List<string> output_web_config = LoadOutputWebConfig();
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                if ((output_web_config != null) && (output_web_config.Count > 0))
                                {
                                    foreach (string c in output_web_config)
                                    {
                                        if (int.Parse(c.Split(',')[1]) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            DataSet ds = (new SQLPart()).GetSingleAvgRd_Web(c.Split(',')[1], c.Split(',')[2], DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                            if (ds != null && ds.Tables[0].Rows.Count > 0)
                                            {
                                                double? avgvalue = double.Parse(ds.Tables[0].Rows[0][0].ToString());
                                                //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                                if (avgvalue == null)
                                                {
                                                    if (ce.par_otw_dst_1.Where(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                    {
                                                        ce.par_otw_dst_1.RemoveAll(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                    }
                                                }
                                                else
                                                {
                                                    ce.AddToPar_otwls_1(c.Split(',')[0].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_Web_2(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            List<string> output_web_config = LoadOutputWebConfig();
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                if ((output_web_config != null) && (output_web_config.Count > 0))
                                {
                                    foreach (string c in output_web_config)
                                    {
                                        if (int.Parse(c.Split(',')[1]) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            DataSet ds = (new SQLPart()).GetSingleAvgRd_Web(c.Split(',')[1], c.Split(',')[2], DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                            if (ds != null && ds.Tables[0].Rows.Count > 0)
                                            {
                                                double? avgvalue = double.Parse(ds.Tables[0].Rows[0][0].ToString());
                                                //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                                if (avgvalue == null)
                                                {
                                                    if (ce.par_otw_dst_2.Where(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                    {
                                                        ce.par_otw_dst_2.RemoveAll(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                    }
                                                }
                                                else
                                                {
                                                    ce.AddToPar_otwls_2(c.Split(',')[0].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_Web_3(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            List<string> output_web_config = LoadOutputWebConfig();
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                if ((output_web_config != null) && (output_web_config.Count > 0))
                                {
                                    foreach (string c in output_web_config)
                                    {
                                        if (int.Parse(c.Split(',')[1]) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            DataSet ds = (new SQLPart()).GetSingleAvgRd_Web(c.Split(',')[1], c.Split(',')[2], DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                            if (ds != null && ds.Tables[0].Rows.Count > 0)
                                            {
                                                double? avgvalue = double.Parse(ds.Tables[0].Rows[0][0].ToString());
                                                //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                                if (avgvalue == null)
                                                {
                                                    if (ce.par_otw_dst_3.Where(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                    {
                                                        ce.par_otw_dst_3.RemoveAll(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                    }
                                                }
                                                else
                                                {
                                                    ce.AddToPar_otwls_3(c.Split(',')[0].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="ce"></param>
        private void HourAvgValue_ForFurnace_Web_4(DateTime st, DateTime et, EPASync.ComparerEngine ce)
        {
            DataSet frunningrd = (new SQLPart()).GetRelatedFurnaceRunningRd(st, et);
            List<string> output_web_config = LoadOutputWebConfig();
            if (frunningrd != null)
            {
                foreach (DataRow dr in frunningrd.Tables[0].Rows)
                {
                    List<StartEndPair> sepl = GetStartEndPair_Hour(DateTime.Parse(dr["starttime"].ToString()), DateTime.Parse(dr["endtime"].ToString()));
                    if (sepl != null)
                    {
                        foreach (StartEndPair sep in sepl)
                        {
                            if ((sep.StartTime <= et) && (sep.StartTime >= st))
                            {
                                // add biz logic
                                if ((dr["grouptype"].ToString() == "") && (sep == sepl.Last()))
                                {
                                    continue;
                                }

                                if ((output_web_config != null) && (output_web_config.Count > 0))
                                {
                                    foreach (string c in output_web_config)
                                    {
                                        if (int.Parse(c.Split(',')[1]) == int.Parse(dr["machineid"].ToString()))
                                        {
                                            DataSet ds = (new SQLPart()).GetSingleAvgRd_Web(c.Split(',')[1], c.Split(',')[2], DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                            if (ds != null && ds.Tables[0].Rows.Count > 0)
                                            {
                                                double? avgvalue = double.Parse(ds.Tables[0].Rows[0][0].ToString());
                                                //if it is null, check whether destination set has the related rd, if it has, delete it from destination set to avoid synchronizing
                                                if (avgvalue == null)
                                                {
                                                    if (ce.par_otw_dst_4.Where(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00"))).Count() > 0)
                                                    {
                                                        ce.par_otw_dst_4.RemoveAll(i => i.pname == c.Split(',')[0].ToString() && i.timestamps == DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")));
                                                    }
                                                }
                                                else
                                                {
                                                    ce.AddToPar_otwls_4(c.Split(',')[0].ToString(), DateTime.Parse(sep.StartTime.ToString("yyyy-MM-dd HH:00:00")), (double)avgvalue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        public List<string> LoadOutputWebConfig()
        {
            try
            {
                List<string> output_web_config = new List<string>();
                AppSettingsReader asr = new AppSettingsReader();
                string filename = (string)asr.GetValue("OutputWeb", typeof(string));
                FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read);
                StreamReader sr = new StreamReader(fs, System.Text.Encoding.Default);
                string tempstr;
                while ((tempstr = sr.ReadLine()) != null)
                {
                    if ((tempstr.Trim() != "") && (tempstr.Trim().Substring(0, 2) != "//"))
                    {
                        output_web_config.Add(tempstr);
                    }
                }
                sr.Close();
                fs.Close();
                return output_web_config;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public class StartEndPair
        {
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public List<StartEndPair> GetStartEndPair_Month(DateTime st, DateTime et)
        {
            List<StartEndPair> pair = new List<StartEndPair>();
            if (st < et)
            {
                DateTime temp = st;
                while (temp < et)
                {
                    if (DateTime.Parse(temp.AddMonths(1).ToString("yyyy-MM-01 00:00:00")) < et)
                    {
                        pair.Add(new StartEndPair() { StartTime = temp, EndTime = DateTime.Parse(temp.AddMonths(1).ToString("yyyy-MM-01 00:00:00")) });
                        temp = DateTime.Parse(temp.AddMonths(1).ToString("yyyy-MM-01 00:00:00"));
                    }
                    else
                    {
                        pair.Add(new StartEndPair() { StartTime = temp, EndTime = et });
                        break;
                    }
                }
                return pair;
            }
            else
            {
                return null;
            }
        }

        // <summary>
        /// 
        /// </summary>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <returns></returns>
        public List<StartEndPair> GetStartEndPair_Hour(DateTime st, DateTime et)
        {
            List<StartEndPair> pair = new List<StartEndPair>();
            if (st < et)
            {
                DateTime temp = st;
                while (temp < et)
                {
                    if (DateTime.Parse(temp.AddHours(1).ToString("yyyy-MM-dd HH:00:00")) < et)
                    {
                        pair.Add(new StartEndPair() { StartTime = temp, EndTime = DateTime.Parse(temp.AddHours(1).ToString("yyyy-MM-dd HH:00:00")) });
                        temp = DateTime.Parse(temp.AddHours(1).ToString("yyyy-MM-dd HH:00:00"));
                    }
                    else
                    {
                        pair.Add(new StartEndPair() { StartTime = temp, EndTime = et });
                        break;
                    }
                }
                return pair;
            }
            else
            {
                return null;
            }
        }

    }
}
