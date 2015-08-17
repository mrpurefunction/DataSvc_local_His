using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LocalPIData
{
    /// <summary>
    /// PI Average Value
    /// </summary>
    public class PIAvgData
    {
        public static string ip = (string)(new System.Configuration.AppSettingsReader()).GetValue("ip", typeof(string));
        public static string user = (string)(new System.Configuration.AppSettingsReader()).GetValue("username", typeof(string));
        public static string psd = (string)(new System.Configuration.AppSettingsReader()).GetValue("password", typeof(string));

        /// <summary>
        /// 构造函数
        /// </summary>
        public PIAvgData()
        {
            new PI.PIFunc2(ip, user, psd);
        }

        /// <summary>
        /// Get Average Value
        /// </summary>
        /// <param name="pn"></param>
        /// <param name="st"></param>
        /// <param name="et"></param>
        /// <param name="shift"></param>
        /// <returns></returns>
        public double? GetAvgValue(string pn, DateTime st, DateTime et, int shift)
        {
            return (new PI.PIFunc2(ip, user, psd)).GetAverageValue(pn, st.AddSeconds(shift), et.AddSeconds(shift));         
        }
    }
}
