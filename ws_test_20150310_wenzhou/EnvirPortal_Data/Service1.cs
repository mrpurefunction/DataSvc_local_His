using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

using System.Threading;

namespace EnvirPortal_Data
{
    public partial class Service1 : ServiceBase
    {
        private object m = new object();
        private bool IsExited = false;

        private PublicLib.TimeMachine tm_min = new PublicLib.TimeMachine(0, 0, 0, 1, 60, 20, PublicLib.OffsetType.Second);

        /// <summary>
        /// realtime thread worker
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
                        (new Business()).AddRemotePIHisData(tm_min.LastTimeStamp, tm_min.LastTimeStamp);
                    }
                    Thread.Sleep(500);
                }
            }
        }

        /// <summary>
        /// history thread worker
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
                    (new Business()).AddRemoteEnvirMonitorData(DateTime.Now.AddDays(-7), DateTime.Now);
                    (new Business()).UpdateRemoteExceptionGroupData(DateTime.Now.AddDays(-7), DateTime.Now);
                    (new Business()).UpdateRemoteExceptionRuleLogMatchData(DateTime.Now.AddDays(-7), DateTime.Now);
                    (new Business()).UpdateRemoteGroupRulesData(DateTime.Now.AddDays(-7), DateTime.Now);
                    Thread.Sleep(5000);
                }
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
        /// start the service
        /// </summary>
        /// <param name="args"></param>
        protected override void OnStart(string[] args)
        {
            Thread realt = new Thread(new ThreadStart(realfn));
            Thread hist = new Thread(new ThreadStart(hisfn));
            lock (m)
            {
                IsExited = false;
            }
            realt.Start();
            //hist.Start();
        }

        /// <summary>
        /// stop the service
        /// </summary>
        protected override void OnStop()
        {
            lock (m)
            {
                if (IsExited == false)
                {
                    IsExited = true;
                }
            }
        }

        /// <summary>
        /// start services from outside app
        /// </summary>
        public void startsvc()
        {
            OnStart(null);
        }

        /// <summary>
        /// stop services from outside app
        /// </summary>
        public void stopsvc()
        {
            OnStop();
        }

        public void test()
        {
            //(new Business()).AddRemoteCalibRdsData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            //(new Business()).AddRemoteCalibRuleValueData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            //(new Business()).AddRemoteEnvirMonitorData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            (new Business()).AddRemoteExceptionGroupData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            (new Business()).AddRemoteExceptionRuleLogMatchData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            (new Business()).AddRemoteGroupRulesData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            (new Business()).AddRemotePIAvgData(new DateTime(2015, 1, 1, 0, 0, 0), new DateTime(2015, 3, 20, 0, 0, 0));
            //DateTime st = new DateTime(2015, 1, 1, 0, 0, 0);
            //DateTime et = new DateTime(2015, 3, 20, 0, 0, 0);
            //while (st < et)
            //{
            //    (new Business()).AddRemotePIHisData(st, st.AddHours(1.0));
            //    st = st.AddHours(1.0);
            //}
        }
    }
}
