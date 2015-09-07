using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data;

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
        public static int plantid = 1;
        /// <summary>
        /// 
        /// </summary>
        public Biz()
        {

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

    }
}
