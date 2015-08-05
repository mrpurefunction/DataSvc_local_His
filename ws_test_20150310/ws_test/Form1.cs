using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using Microsoft.Practices.EnterpriseLibrary.Data;
using Microsoft.Practices.EnterpriseLibrary.Common;

namespace ws_test
{
    public partial class Form1 : Form
    {
        //private cems.cemsSoapClient c;
        public Form1()
        {
            InitializeComponent();
            //c = new cems.cemsSoapClient("cemsSoap");//,"http://localhost:2084/cems.asmx"
            //new PI.PIFunc2("10.150.124.193", "pirw", "pirw");


            //(new PI.PIFunc2(null, null, null)).DisconnectPI();
        }

        #region
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            //c.AddPiRecord(new DateTime(2010, 1, 1, 0, 0, 0), "abcd", 0.4f, 1, 1);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button2_Click(object sender, EventArgs e)
        {
            //c.AddRuleLogRd(new DateTime(2010, 1, 1, 0, 0, 0), "abcd", "zzz", "复杂规则", "let us go", 1, 1);
        }
        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button3_Click(object sender, EventArgs e)
        {
            try
            {
                DateTime st = DateTime.Parse(textBox1.Text);
                DateTime et = DateTime.Parse(textBox2.Text);

                StringBuilder sb = new StringBuilder();
                sb.Append("select pointname,machineid from Point_Machine_Map");
                Database db = DatabaseFactory.CreateDatabase("dbconn");
                System.Data.Common.DbCommand dbc = db.GetSqlStringCommand(sb.ToString());
                DataSet ds = db.ExecuteDataSet(dbc);
                DataTable dt = ds.Tables[0];
                List<point_machine> pmlist = new List<point_machine>();
                foreach (DataRow dr in dt.Rows)
                {
                    point_machine pm = new point_machine();
                    pm.pn = dr["pointname"].ToString();
                    pm.mid = int.Parse(dr["machineid"].ToString());
                    pmlist.Add(pm);
                }

                LocalPIData.SQLPart s = new LocalPIData.SQLPart();

                while (st <= et)
                {
                    foreach (point_machine p in pmlist)
                    {
                        PI.RetVal rtv = (new PI.PIFunc2("10.150.124.193", "pirw", "pirw")).GetPointHisValue(p.pn, st);
                        if (rtv != null)
                        {
                            
                            s.AddPiRecord(st, p.pn, (float)rtv.pvalue, p.mid, 1);
                            //c.AddPiRecord(st, p.pn, (float)rtv.pvalue, p.mid, 1);
                        }
                    }
                    st = st.AddMinutes(1);
                }
                (new PI.PIFunc2(null, null, null)).DisconnectPI();
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Start LocalPIData
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button4_Click(object sender, EventArgs e)
        {
            (new LocalPIData.Service1()).startsvc();
        }

        /// <summary>
        /// Stop LocalPIData
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button5_Click(object sender, EventArgs e)
        {
            (new LocalPIData.Service1()).stopsvc();
        }

        /// <summary>
        /// Start RemotePortal
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button6_Click(object sender, EventArgs e)
        {
            (new EnvirPortal_Data.Service1()).startsvc();
        }

        /// <summary>
        /// Stop RemotePortal
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button7_Click(object sender, EventArgs e)
        {
            (new EnvirPortal_Data.Service1()).stopsvc();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button8_Click(object sender, EventArgs e)
        {
            //(new LocalPIData.Service1()).test();
            (new EnvirPortal_Data.Service1()).test();
        }
    }

    public class point_machine
    {
        public string pn;
        public int mid;
    }
}
