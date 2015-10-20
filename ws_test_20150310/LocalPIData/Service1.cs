using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

using System.Threading;
using EPASync;

namespace LocalPIData
{
    public partial class Service1 : ServiceBase
    {
        private object m = new object();
        private bool IsExited = false;

        private PublicLib.TimeMachine tm_min = new PublicLib.TimeMachine(0, 0, 0, 1, 60, 20, PublicLib.OffsetType.Second);
        private PublicLib.TimeMachine tm_hour = new PublicLib.TimeMachine(0, 0, 0, 1, 3600, 360, PublicLib.OffsetType.Second);

        Thread realt;
        Thread hist;

       /// <summary>
       /// realtime data
       /// </summary>
        public void realfn()
        {
            bool exitsig;
            while (1 == 1)
            {
                lock (m)
                {
                    exitsig = IsExited;
                }
                if (exitsig == true)
                {
                    break;
                }
                else
                {
                    if (tm_min.IsPermitted())
                    {
                        (new Biz()).RealTimeBiz(tm_min.LastTimeStamp);
                        //(new Biz()).RealTimeBiz_avg(DateTime.Now);
                    }
                    if (tm_hour.IsPermitted())
                    {
                        (new Biz()).RealTimeBiz_avg(tm_hour.LastTimeStamp);
                    }
                    Thread.Sleep(500);
                }
            }
        }

        /// <summary>
        /// supply his data
        /// </summary>
        public void hisfn()
        {
            bool exitsig;
            while (1 == 1)
            {
                lock (m)
                {
                    exitsig = IsExited;
                }
                if (exitsig == true)
                {
                    break;
                }
                else
                {
                    //(new Biz()).CalibSpanBiz(DateTime.Now.AddMonths(-1), DateTime.Now/*.AddMonths(-3).AddDays(90)*/);
                    //(new Biz()).CalibRuleValueBiz(DateTime.Parse(DateTime.Now.AddMonths(-1).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")));
                    //(new Biz()).CalibRuleValueBiz_Outside(DateTime.Parse(DateTime.Now.AddMonths(-1).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")));

                    //update the above code to sync mode to avoid t_rulelogs modification
                    EPASync.ComparerEngine ce = new ComparerEngine();
                    DateTime ts = DateTime.Now;

                    //output total
                    ce.InitPar_otls(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, 0, null);
                    (new Biz()).HourAvgValue_ForFurnace(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, ce);
                    ce.MarkPar_otls();
                    ce.CommitPar_otls();

                    //output total (web 2.0 data)
                    ce.InitPar_otwls(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, 0, null);
                    (new Biz()).HourAvgValue_ForFurnace_Web(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, ce);
                    ce.MarkPar_otwls();
                    ce.CommitPar_otwls();

                    //output total remote
                    ce.InitPar_otls2(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, Biz.plantid, null);
                    ce.MarkPar_otls2();
                    ce.CommitPar_otls2();

                    //output total (web 2.0) remote
                    ce.InitPar_otlsw2(DateTime.Parse(ts.AddMonths(-2).ToString("yyyy-MM-01 00:00:00")), ts, Biz.plantid, null);
                    ce.MarkPar_otwls2();
                    ce.CommitPar_otwls2();



                    (new Biz()).CalibSpanBiz_Sync(ts.AddMonths(-3), ts, ce);
                    //add something for select existed rds
                    ce.InitCrls2(ts.AddMonths(-3), ts, 0, new int[] { 0 });
                    ce.MarkCrls2();
                    ce.CommitCrls2();

                    (new Biz()).CalibRuleValueBiz_Sync(DateTime.Parse(ts.AddMonths(-3).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(ts.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")), ce);
                    (new Biz()).CalibRuleValueBiz_Outside_Sync(DateTime.Parse(ts.AddMonths(-3).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(ts.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")), ce);
                    //add something for select existed rds
                    ce.InitCrvls2(DateTime.Parse(ts.AddMonths(-3).ToString("yyyy-MM-dd HH:00:00")).AddHours(-3.0), DateTime.Parse(ts.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")).AddHours(-1.0), 0, new int[] { 0 });
                    ce.MarkCrvls2();
                    ce.CommitCrvls2();

                    (new Biz()).RunningAsync_Month(DateTime.Parse(ts.AddMonths(-6).ToString("yyyy-MM-01 00:00:00")), ts, ce);
                    ce.InitAscrls(DateTime.Parse(ts.AddMonths(-6).ToString("yyyy-MM-01 00:00:00")), ts, 0, null);
                    ce.MarkAscrls();
                    ce.CommitAscrls();

                    (new Biz()).MachineStopStatistic_Month(DateTime.Parse(ts.AddMonths(-6).ToString("yyyy-MM-01 00:00:00")), ts, ce);
                    ce.InitMsls(DateTime.Parse(ts.AddMonths(-6).ToString("yyyy-MM-01 00:00:00")), ts, 0, null);
                    ce.MarkMsls();
                    ce.CommitMsls();

                    //init dst dataset first
                    ce.InitHals(DateTime.Parse(ts.AddMonths(-3).ToString("yyyy-MM-01 00:00:00")), ts, 0, null);
                    (new Biz()).HourAvgValue_Month(DateTime.Parse(ts.AddMonths(-3).ToString("yyyy-MM-01 00:00:00")), ts, ce);
                    ce.MarkHals();
                    ce.CommitHals();

                    //calculate report value
                    (new MonthAvgCalculation.Biz()).SyncHourAvg_Month(-3, ce);

                    
                    //modified 2015/05/13 modified again 2015/05/21
                    (new Biz()).HistoryBiz(DateTime.Parse(DateTime.Now.AddDays(-14).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(DateTime.Now/*.AddDays(-1)*/.AddHours(-1.0).ToString("yyyy-MM-dd HH:00:00")));
                    (new Biz()).HistoryBiz_avg(DateTime.Parse(DateTime.Now.AddDays(-21).ToString("yyyy-MM-dd HH:00:00")), DateTime.Parse(DateTime.Now.AddDays(-1).ToString("yyyy-MM-dd HH:00:00")));                  
                    Thread.Sleep(5000);
                }
                //
                //break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Service1()
        {
            InitializeComponent();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="args"></param>
        protected override void OnStart(string[] args)
        {
            realt = new Thread(new ThreadStart(realfn));
            hist = new Thread(new ThreadStart(hisfn));
            lock (m)
            {
                IsExited = false;
            }
            //realt.Start();
            hist.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        protected override void OnStop()
        {
            try
            {
                //realt.Abort();
                hist.Abort();
            }
            catch (Exception ex)
            {
            }
            //lock (m)
            //{
            //    if (IsExited == false)
            //    {
            //        IsExited = true;
            //    }
            //}
        }

        /// <summary>
        /// start service from outside app
        /// </summary>
        public void startsvc()
        {
            OnStart(null);
        }

        /// <summary>
        /// stop service from outside app
        /// </summary>
        public void stopsvc()
        {
            OnStop();
        }


        public void test()
        {
            (new Biz()).CalibRuleValueBiz_Outside(new DateTime(2015, 3, 5), new DateTime(2015, 3, 7));
        }
    }
}
