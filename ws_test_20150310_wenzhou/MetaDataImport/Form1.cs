using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

namespace MetaDataImport
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        /// <summary>
        /// calib points
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCalibPoints();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCalibPoint(dr["pointname"].ToString(), int.Parse(dr["plantid"].ToString()), int.Parse(dr["machine"].ToString()));
                }
            }
        }

        /// <summary>
        /// chartpointconfig
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button2_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetChartPointConfig();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddChartPointConfig(dr["pointname"].ToString(), 1, dr["type"].ToString(), int.Parse(dr["id"].ToString()));
                }
            }
        }

        /// <summary>
        /// cpc_fgd_calib
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button3_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_F_C();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_F_C(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()));
                }
            }
        }

        /// <summary>
        /// cpc_fgd_launch
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button4_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_F_L();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_F_L(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()));
                }
            }
        }

        /// <summary>
        /// cpc_fgd_scon
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button5_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_F_S();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_F_S(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()), dr["ud1"].ToString());
                }
            }
        }

        /// <summary>
        /// cpc_scr_calib
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button6_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_S_C();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_S_C(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()));
                }
            }
        }

        /// <summary>
        /// cpc_scr_launch
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button7_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_S_L();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_S_L(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()), dr["ud1"].ToString());
                }
            }
        }

        /// <summary>
        /// cpc_scr_scon
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button8_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetCPC_S_S();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddCPC_S_S(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()), dr["ud1"].ToString());
                }
            }
        }

        /// <summary>
        /// PIHourAvgPoints
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button9_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetHourAvgPoints();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddHourAvgPoint(dr["pointname"].ToString(), 1, int.Parse(dr["shiftsecs"].ToString()));
                }
            }
        }

        /// <summary>
        /// point_machine_map
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button10_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetPMM();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddPMM(dr["pointname"].ToString(), 1, int.Parse(dr["id"].ToString()), int.Parse(dr["machineid"].ToString()), int.Parse(dr["enabled"].ToString()), dr["description"].ToString());
                }
            }
        }

        /// <summary>
        /// relevantpoints
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button11_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetRelevantPoints();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddRelevantPoint(dr["pointname"].ToString(), 1, dr["pointtype"].ToString(), int.Parse(dr["machineid"].ToString()));
                }
            }
        }

        /// <summary>
        /// rule_machine_map
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button12_Click(object sender, EventArgs e)
        {
            DataSet ds = (new SQL()).GetRMM();
            if (ds != null)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    (new SQL()).AddRMM(dr["rulename"].ToString(), 1, int.Parse(dr["id"].ToString()), int.Parse(dr["machineid"].ToString()), int.Parse(dr["enabled"].ToString()));
                }
            }
        }


    }
}
